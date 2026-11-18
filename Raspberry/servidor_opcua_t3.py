#!/usr/bin/env python3
"""
Servidor OPC UA - dispositivo 2 | TP3 Integradora | Protocolos de Comunicaciones Industriales | UNRaf
Raspberry Pi 4: maestro MODBUS RTU (RS485) hacia el esclavo (dispositivo 1) + servidor OPC UA hacia el
cliente (dispositivo 3). Adaptacion del servidor del TP2: se mantienen la seguridad (3 endpoints), la
trust list y los usuarios; desaparecen la rampa y los LEDs, y el espacio de direcciones sale de la tabla
de variables del esclavo (config_esclavo.json).

Bloques del diagrama de flujo (para trazabilidad en el informe):
  S1 - Inicializacion (configuracion, maestro Modbus, servidor, endpoint, certificado, politicas de
       seguridad, trust list, usuarios)
  S2 - Creacion del espacio de direcciones (namespace, objetos, variables) a partir de la tabla de variables
  S3 - Ciclo de polling Modbus y publicacion de los cambios en los nodos            (puente_opcua.al_ciclo)
  S4 - Estado de la comunicacion Modbus: Estado, contadores y Alcanzable             (puente_opcua.al_ciclo)
  S5 - Comandos del cliente OPC UA sobre los nodos de salida -> escritura Modbus     (puente_opcua P2/P3)

Espacio de direcciones (namespace urn:unraf:pdci:t3, ns=1; NodeId de tipo string):
  Esclavo_Modbus_ID<N>.Inputs.<nombre>     entradas del esclavo        (solo lectura)
  Esclavo_Modbus_ID<N>.Outputs.<nombre>    salidas del esclavo         (lectura/escritura)
  Status.MODBUS.Estado                     String: DONE | CRC ERROR | DEVICE NOT REACHED
  Status.MODBUS.OK / CRC_ERROR / TIMEOUT   UInt32: contadores de peticiones (consigna 1.c)
  Status.MODBUS.Alcanzable                 Boolean: False tras N ciclos sin ninguna respuesta
                                           (el dispositivo 3 lo publica como DDEATH)

Endpoints publicados (igual que el TP2): SecurityMode None, Sign y SignAndEncrypt (Basic256Sha256).
Identidades aceptadas: anonimo y usuario/contrasena.

Requisitos: asyncua==1.1.8, cryptography, pyserial
Archivos de seguridad (los mismos del TP2; los genera generar_certificados.py servidor):
  pki/servidor-cert.der, pki/servidor-key.pem, pki/confianza/*.der

Uso:
  python servidor_opcua_t3.py
  python servidor_opcua_t3.py --puerto /dev/ttyUSB0 --baudrate 9600 --id 1
  python servidor_opcua_t3.py --usuario ana:1234 --debug
"""
import argparse
import asyncio
import inspect
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from asyncua import Server, ua
from asyncua.crypto.truststore import TrustStore
from asyncua.crypto.validator import CertificateValidator, CertificateValidatorOptions
from asyncua.server.user_managers import UserManager
try:
    from asyncua.server.users import User, UserRole
except ImportError:
    try:
        from asyncua.crypto.permission_rules import User, UserRole
    except ImportError:
        from asyncua.server.user_managers import User, UserRole

import maestro_modbus as mm
from puente_opcua import ESTADO_INICIAL, Puente

# ---------------------------------------------------------------- S1: constantes
NS_URI = "urn:unraf:pdci:t3"                  # URI del namespace de este TP (ns=1)
# ApplicationUri del servidor: DEBE coincidir con el subjectAltName del certificado. Se deja el del TP2
# para reutilizar el certificado ya instalado; si se regenera, usar --application-uri.
APPLICATION_URI = "urn:unraf:pdci:t2"
ENDPOINT = "opc.tcp://0.0.0.0:4840"           # escucha en todas las interfaces
USUARIO_DEFECTO = "operador:pdci2026"         # usuario/contrasena de laboratorio (igual que el TP2)

_log = logging.getLogger("servidor")


def ahora_utc() -> datetime:
    return datetime.now(timezone.utc)


async def _maybe_await(resultado):
    """Algunas funciones de asyncua son sincronas o corrutinas segun la version."""
    if inspect.isawaitable(resultado):
        return await resultado
    return resultado


# ---------------------------------------------------------------- S1: autenticacion de usuario (del TP2)
class GestorUsuarios(UserManager):
    """Acepta identidad anonima y usuario/contrasena (UserIdentityToken de ActivateSession).
    Cualquier otra combinacion se rechaza (BadIdentityTokenRejected / BadUserAccessDenied)."""

    def __init__(self, usuarios: dict):
        super().__init__()
        self.usuarios = usuarios

    def get_user(self, iserver, username=None, password=None, certificate=None):
        # Un certificado vacio (b"" en modo None) cuenta como "sin certificado"
        if username is None and password is None and not certificate:
            _log.info("ActivateSession: identidad ANONIMA")
            return User(role=UserRole.Admin)
        if username is not None:
            if self.usuarios.get(username) == password:
                _log.info("ActivateSession: usuario '%s' autenticado", username)
                return User(role=UserRole.Admin)
            _log.warning("ActivateSession: usuario '%s' RECHAZADO (credenciales invalidas)", username)
            return None
        # Sin usuario, pero con algun otro dato de identidad (p. ej. el certificado del canal seguro,
        # que ya paso la trust list al abrirlo).
        tam = (len(certificate) if isinstance(certificate, (bytes, bytearray))
               else int(certificate is not None))
        _log.info("ActivateSession: sin usuario/contrasena; certificado recibido: %d bytes; "
                  "contrasena recibida=%s", tam, password is not None)
        return User(role=UserRole.Admin)


async def configurar_seguridad(server: Server, pki: Path, confianza: Path, application_uri: str):
    """S1: certificado + clave del servidor, trust list y los tres endpoints (del TP2).
    Si faltan los archivos del servidor se publica solo SecurityMode None (aviso)."""
    cert = pki / "servidor-cert.der"
    clave = pki / "servidor-key.pem"

    if not (cert.is_file() and clave.is_file()):
        _log.warning("No se encontraron %s / %s: se publica SOLO el endpoint None. "
                     "Ejecute generar_certificados.py servidor.", cert, clave)
        server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
        return False

    # El ApplicationUri del servidor debe coincidir con el del subjectAltName del certificado
    await _maybe_await(server.set_application_uri(application_uri))
    await server.load_certificate(str(cert))
    await server.load_private_key(str(clave))

    # Trust list: carpeta con los certificados .der de los clientes en los que se confia
    confianza.mkdir(parents=True, exist_ok=True)
    trust_store = TrustStore([confianza], [])
    await trust_store.load()
    opciones = getattr(CertificateValidatorOptions, "TRUSTED_VALIDATION",
                       CertificateValidatorOptions.EXT_VALIDATION)
    validador = CertificateValidator(opciones | CertificateValidatorOptions.PEER_CLIENT, trust_store)

    server.set_security_policy(
        [ua.SecurityPolicyType.NoSecurity,
         ua.SecurityPolicyType.Basic256Sha256_Sign,
         ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt])
    server.set_certificate_validator(validador.validate)
    n = len(list(confianza.glob("*.der")))
    _log.info("Seguridad: None + Sign + SignAndEncrypt (Basic256Sha256); "
              "trust list %s con %d certificado(s)", confianza, n)
    return True


# ---------------------------------------------------------------- S2: espacio de direcciones
def tipos_ua() -> dict:
    return {"Boolean": ua.VariantType.Boolean, "UInt16": ua.VariantType.UInt16,
            "Int16": ua.VariantType.Int16, "UInt32": ua.VariantType.UInt32,
            "String": ua.VariantType.String}


def valor_inicial(tipo: str):
    return False if tipo == "Boolean" else ("" if tipo == "String" else 0)


def convertir(tipo: str, valor):
    if tipo == "Boolean":
        return bool(valor)
    if tipo == "String":
        return str(valor)
    return int(valor)


class NodoUA:
    """Nodo de asyncua con el metodo publicar() que espera el puente. Cada publicacion lleva el
    SourceTimestamp del instante en que el dato se genero (como en el TP2)."""

    def __init__(self, nodo, tipo: str):
        self.nodo = nodo
        self.tipo = tipo
        self._vtype = tipos_ua()[tipo]

    async def publicar(self, valor):
        await self.nodo.write_value(
            ua.DataValue(ua.Variant(convertir(self.tipo, valor), self._vtype),
                         SourceTimestamp=ahora_utc()))


async def crear_espacio(server: Server, idx: int, poller: mm.Poller, id_dispositivo: str):
    """Crea los objetos y variables. Devuelve (nodos_datos, nodos_estado, nombres_escribibles, tabla)."""
    tipos = tipos_ua()
    objetos = server.nodes.objects
    tabla = []                                  # filas para el informe: NodeId, tipo, acceso, origen

    async def crear_objeto(padre, nodeid: str, nombre: str):
        return await padre.add_object(ua.NodeId(nodeid, idx), ua.QualifiedName(nombre, idx))

    async def crear_variable(padre, nodeid: str, nombre: str, tipo: str, inicial, escribible: bool,
                             origen: str):
        var = await padre.add_variable(ua.NodeId(nodeid, idx), ua.QualifiedName(nombre, idx),
                                       ua.Variant(inicial, tipos[tipo]))
        if escribible:
            await var.set_writable()            # AccessLevel = CurrentRead | CurrentWrite
        tabla.append((f"ns={idx};s={nodeid}", tipo,
                      "Lectura/Escritura" if escribible else "Solo lectura", origen))
        return var

    # --- variables del esclavo (dispositivo 1)
    obj_dispositivo = await crear_objeto(objetos, id_dispositivo, id_dispositivo)
    grupos = {}
    nodos_datos = {}
    nombres_escribibles = {}                    # identificador del NodeId -> nombre de variable
    nodos_escribibles = []
    for v in poller.variables:
        partes = v.nombre.split("/")
        padre, ruta = obj_dispositivo, id_dispositivo
        for grupo in partes[:-1]:
            ruta += "." + grupo
            if ruta not in grupos:
                grupos[ruta] = await crear_objeto(padre, ruta, grupo)
            padre = grupos[ruta]
        nodeid = id_dispositivo + "." + ".".join(partes)
        info = AREAS_TEXTO(v)
        var = await crear_variable(padre, nodeid, partes[-1], v.tipo, valor_inicial(v.tipo),
                                   v.escribible, info)
        nodos_datos[v.nombre] = NodoUA(var, v.tipo)
        if v.escribible:
            nombres_escribibles[nodeid] = v.nombre
            nodos_escribibles.append(var)

    # --- estado de la comunicacion Modbus (generado por este dispositivo)
    obj_status = await crear_objeto(objetos, "Status", "Status")
    obj_modbus = await crear_objeto(obj_status, "Status.MODBUS", "MODBUS")
    tipos_estado = {"Estado": "String", "OK": "UInt32", "CRC_ERROR": "UInt32", "TIMEOUT": "UInt32",
                    "Alcanzable": "Boolean"}
    nodos_estado = {}
    for clave, tipo in tipos_estado.items():
        var = await crear_variable(obj_modbus, f"Status.MODBUS.{clave}", clave, tipo,
                                   ESTADO_INICIAL[clave], False, "estado del maestro Modbus")
        nodos_estado[clave] = NodoUA(var, tipo)

    return nodos_datos, nodos_estado, nombres_escribibles, nodos_escribibles, tabla


def AREAS_TEXTO(v: "mm.Variable") -> str:
    info = mm.AREAS[v.area]
    fcs = f"FC{info['fc_lectura']:02d}" + (f"/FC{info['fc_escritura']:02d}" if info["fc_escritura"] else "")
    return f"{v.area} dir {v.direccion} ({fcs})"


# ---------------------------------------------------------------- S5: escrituras del cliente OPC UA
class ManejadorEscrituras:
    """Se ejecuta cada vez que cambia un nodo de salida (escritura de un cliente OPC UA, o publicacion
    del propio puente: esta ultima la descarta el puente por ser un eco)."""

    def __init__(self, puente: Puente, nombres: dict):
        self.puente = puente
        self.nombres = nombres

    def datachange_notification(self, node, val, data):
        if val is None:
            return
        nombre = self.nombres.get(node.nodeid.Identifier)
        if nombre is not None:
            self.puente.al_cambiar_nodo(nombre, val)


# ---------------------------------------------------------------- S1 + bucle principal
async def main():
    parser = argparse.ArgumentParser(description="Servidor OPC UA + maestro Modbus (TP3 PdCI)")
    parser.add_argument("--config", default="config_esclavo.json")
    parser.add_argument("--puerto")
    parser.add_argument("--baudrate", type=int)
    parser.add_argument("--paridad", choices=("N", "E", "O"))
    parser.add_argument("--bits-parada", type=int, choices=(1, 2))
    parser.add_argument("--id", type=int, help="ID Modbus del esclavo")
    parser.add_argument("--timeout-ms", type=int)
    parser.add_argument("--polling-ms", type=int)
    parser.add_argument("--pki", default="pki", help="carpeta con el certificado y la clave")
    parser.add_argument("--confianza", default=None, help="trust list (por defecto: <pki>/confianza)")
    parser.add_argument("--application-uri", default=APPLICATION_URI,
                        help="debe coincidir con el subjectAltName del certificado del servidor")
    parser.add_argument("--usuario", action="append", default=[],
                        help="usuario:contrasena (repetible; por defecto " + USUARIO_DEFECTO + ")")
    parser.add_argument("--debug", action="store_true", help="muestra cada trama Modbus TX/RX")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("asyncua").setLevel(logging.WARNING)       # asyncua registra cada peticion en INFO
    if args.debug:
        logging.getLogger("modbus").setLevel(logging.DEBUG)

    usuarios = {}
    for par in (args.usuario or [USUARIO_DEFECTO]):
        nombre, _, clave = par.partition(":")
        usuarios[nombre] = clave

    # ------------------------------------------------------------ S1: maestro Modbus y servidor
    try:
        cfg = mm.cargar_config(args.config, args)
        maestro, poller = mm.crear_desde_config(cfg)
    except (OSError, ValueError, KeyError, TypeError) as e:
        print(f"Error de configuracion: {e}", file=sys.stderr)
        return 2
    id_dispositivo = f"Esclavo_Modbus_ID{poller.esclavo_id}"

    server = Server(user_manager=GestorUsuarios(usuarios))
    await server.init()
    server.set_endpoint(ENDPOINT)
    server.set_server_name("PdCI T3 - Servidor OPC UA (Raspberry Pi)")
    pki = Path(args.pki)
    confianza = Path(args.confianza) if args.confianza else pki / "confianza"
    await configurar_seguridad(server, pki, confianza, args.application_uri)

    # ------------------------------------------------------------ S2: espacio de direcciones
    # Se busca que el namespace quede en el indice 1 (ns=1), como en el TP2.
    ns_node = server.get_node(ua.ObjectIds.Server_NamespaceArray)
    ns_array = await ns_node.read_value()
    if NS_URI not in ns_array and len(ns_array) >= 2:
        ns_array[1] = NS_URI
        await ns_node.write_value(ns_array)
    idx = await server.register_namespace(NS_URI)
    if idx != 1:
        _log.warning("El namespace quedo en indice %d y no en 1; los NodeIds seran ns=%d;s=...", idx, idx)
    _log.info("Namespace %s -> indice %d", NS_URI, idx)

    nodos_datos, nodos_estado, nombres, escribibles, tabla = await crear_espacio(
        server, idx, poller, id_dispositivo)
    _log.info("Espacio de direcciones (%d nodos):", len(tabla))
    for nodeid, tipo, acceso, origen in tabla:
        _log.info("  %-48s %-8s %-18s %s", nodeid, tipo, acceso, origen)

    # ------------------------------------------------------------ S3-S5: puente con el maestro Modbus
    puente = Puente(poller, nodos_datos, nodos_estado,
                    consola=lambda res: print(mm.linea_consola(res), flush=True))
    manejador = ManejadorEscrituras(puente, nombres)
    sub = await server.create_subscription(50, manejador)       # periodo 50 ms
    await sub.subscribe_data_change(escribibles)

    _log.info("Servidor listo. Endpoint anunciado: %s | Modbus: %s %d bps, esclavo ID %d",
              ENDPOINT, maestro.puerto, maestro.baudrate, poller.esclavo_id)
    async with server:
        try:
            await puente.correr(int(cfg["polling_ms"]) / 1000.0)
        finally:
            maestro.cerrar()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        pass
