#!/usr/bin/env python3
"""
Gateway OPC UA -> MQTT Sparkplug B (dispositivo 3) - TP3 Integradora | PdCI | UNRaf
Cliente OPC UA del TP2 (hacia el servidor del dispositivo 2) + Edge Node Sparkplug B (hacia el broker).

Bloques del diagrama de flujo (para trazabilidad en el informe):
  G1 - Inicializacion: configuracion, tabla de variables, Edge Node, seguridad OPC UA
  G2 - Sesion OPC UA (como C1/C6 del TP2): conexion, lectura inicial, suscripcion a los nodos, vigilancia
       y reconexion automatica; los cambios de valor se entregan al Edge Node
  G3 - Sesion MQTT: bdSeq nuevo + Last Will (NDEATH) -> CONNECT -> suscripcion a NCMD/DCMD -> NBIRTH/DBIRTH;
       ante un corte, nueva conexion con bdSeq incrementado
  G4 - Comandos: DCMD -> escritura del nodo OPC UA (el esclavo confirma por la suscripcion -> DDATA)
  G5 - Consola: estadisticas de todas las capas (Modbus, OPC UA, MQTT, Sparkplug B)

Requisitos: asyncua==1.1.8, paho-mqtt>=2.0, protobuf, cryptography
Uso:
  python gateway_sparkplug.py --config config_gateway.json
  python gateway_sparkplug.py --broker xxxx.s1.eu.hivemq.cloud --mqtt-usuario ana --mqtt-clave ****
  python gateway_sparkplug.py --seguridad encrypt --usuario operador --clave pdci2026
"""
import argparse
import asyncio
import inspect
import json
import logging
import os
import ssl
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import paho.mqtt.client as mqtt
from asyncua import Client, ua

import edge_node as en

# ---------------------------------------------------------------- G1: constantes
APPLICATION_URI = "urn:unraf:pdci:t2:cliente"   # debe coincidir con el subjectAltName del certificado del cliente
NS_IDX = 1
PUBLISHING_INTERVAL_MS = 100
KEEPALIVE_COUNT = 20
LIFETIME_COUNT = 100
QUEUE_SIZE = 1
SESSION_TIMEOUT_MS = 30000
REQUEST_TIMEOUT_S = 2
VIGILANCIA_S = 2.0               # lectura periodica de un nodo: detecta caidas del enlace OPC UA
ESPERA_MAX_S = 10.0
MODOS_SEGURIDAD = {"sign": "Sign", "encrypt": "SignAndEncrypt"}
POLITICA = "Basic256Sha256"
TIPOS_UA = {"Boolean": ua.VariantType.Boolean, "UInt16": ua.VariantType.UInt16,
            "Int16": ua.VariantType.Int16, "UInt32": ua.VariantType.UInt32,
            "String": ua.VariantType.String}

_log = logging.getLogger("gateway")


def ahora_utc() -> datetime:
    return datetime.now(timezone.utc)


async def _maybe_await(resultado):
    if inspect.isawaitable(resultado):
        return await resultado
    return resultado


class Estado:
    """Contadores de las capas OPC UA y MQTT para la consola (las de Modbus y Sparkplug B salen del nodo)."""

    def __init__(self):
        self.opc_conectado = False
        self.opc_reconexiones = 0
        self.opc_escrituras_ok = 0
        self.opc_escrituras_error = 0
        self.mqtt_conectado = False
        self.mqtt_intentos_fallidos = 0
        self.mqtt_recibidos = 0


# ================================================================ G2: OPC UA
def nodeid_de(dispositivo: str, nombre: str) -> str:
    """Inputs/Pulsador -> Esclavo_Modbus_ID1.Inputs.Pulsador (el mismo que arma el servidor)."""
    return f"{dispositivo}.{nombre.replace('/', '.')}"


class ManejadorSuscripcion:
    """Notificaciones del servidor OPC UA -> Edge Node (se ejecuta dentro del bucle de asyncio)."""

    def __init__(self, edge: en.NodoBorde, por_identificador: Dict[str, str]):
        self.edge = edge
        self.por_id = por_identificador            # identificador de NodeId -> nombre de variable
        self.estado_ids = {f"Status.MODBUS.{c}": c for c in en.CLAVES_MODBUS}
        self.id_alcanzable = "Status.MODBUS.Alcanzable"

    def datachange_notification(self, node, val, data):
        if val is None:
            return
        ident = node.nodeid.Identifier
        try:
            ts = data.monitored_item.Value.SourceTimestamp
            ts_ms = int(ts.timestamp() * 1000) if ts is not None else None
        except AttributeError:
            ts_ms = None
        if ident in self.por_id:
            self.edge.dato(self.por_id[ident], val, ts_ms)
        elif ident in self.estado_ids:
            self.edge.estado_modbus(self.estado_ids[ident], val)
        elif ident == self.id_alcanzable:
            self.edge.modbus_alcanzable(bool(val))

    def status_change_notification(self, status):
        _log.warning("StatusChangeNotification de la suscripcion: %s", status)


async def configurar_seguridad(client: Client, cfg: Dict[str, Any]):
    """Identica a la del cliente del TP2: ApplicationUri, canal seguro e identidad."""
    client.application_uri = APPLICATION_URI
    if cfg["seguridad"] != "none":
        pki = Path(cfg["pki"])
        cert, clave = pki / "cliente-cert.der", pki / "cliente-key.pem"
        cert_srv = Path(cfg.get("cert_servidor") or pki / "servidor-cert.der")
        for archivo in (cert, clave, cert_srv):
            if not archivo.is_file():
                raise FileNotFoundError(f"falta {archivo} (ver generar_certificados.py)")
        cadena = f"{POLITICA},{MODOS_SEGURIDAD[cfg['seguridad']]},{cert},{clave},{cert_srv}"
        await _maybe_await(client.set_security_string(cadena))
    if cfg.get("usuario"):
        client.set_user(cfg["usuario"])
        client.set_password(cfg.get("clave") or "")


async def vigilante(nodo):
    """Lee un nodo cada VIGILANCIA_S: si el enlace se cayo, la lectura da error y se reconecta."""
    while True:
        await nodo.read_value()
        await asyncio.sleep(VIGILANCIA_S)


async def escritor(cola: asyncio.Queue, edge: en.NodoBorde, nodos: Dict[str, Any], estado: Estado):
    """G4: toma los comandos de a uno y escribe el nodo OPC UA con SourceTimestamp del gateway."""
    while True:
        nombre, valor = await cola.get()
        tipo = edge.variables[nombre].tipo
        try:
            await nodos[nombre].write_value(
                ua.DataValue(ua.Variant(valor, TIPOS_UA[tipo]), SourceTimestamp=ahora_utc()))
            estado.opc_escrituras_ok += 1
            _log.info("Comando escrito en OPC UA: %s = %s", nombre, valor)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            estado.opc_escrituras_error += 1
            _log.warning("Escritura OPC UA %s = %s fallo: %s: %s", nombre, valor, type(e).__name__, e)
            edge.reponer(nombre)          # el Host vuelve a ver el valor real
            if isinstance(e, (ConnectionError, asyncio.TimeoutError)):
                raise                     # enlace caido: que reconecte el bucle de sesion


async def correr_juntas(*corrutinas):
    tareas = [asyncio.create_task(c) for c in corrutinas]
    try:
        hechas, pendientes = await asyncio.wait(tareas, return_when=asyncio.FIRST_EXCEPTION)
        for t in pendientes:
            t.cancel()
        for t in hechas:
            t.result()
    finally:
        for t in tareas:
            t.cancel()


async def sesion_opcua(cfg: Dict[str, Any], dispositivo: str, edge: en.NodoBorde,
                       cola: asyncio.Queue, estado: Estado, fabrica_cliente=None):
    """G2: conexion, lectura inicial, suscripcion y vigilancia; reconecta con espera creciente."""
    espera = 1.0
    ids_datos = {nombre: nodeid_de(dispositivo, nombre) for nombre in edge.variables}
    por_id = {ident: nombre for nombre, ident in ids_datos.items()}
    ids_estado = {c: f"Status.MODBUS.{c}" for c in en.CLAVES_MODBUS + ("Alcanzable",)}
    while True:
        client = (fabrica_cliente or Client)(cfg["url"], timeout=REQUEST_TIMEOUT_S)
        client.session_timeout = SESSION_TIMEOUT_MS
        try:
            await configurar_seguridad(client, cfg)
            _log.info("Conectando a %s (seguridad %s, identidad %s) ...", cfg["url"], cfg["seguridad"],
                      cfg.get("usuario") or "anonima")
            await asyncio.wait_for(client.connect(), timeout=5)
            nodos = {n: client.get_node(f"ns={NS_IDX};s={i}") for n, i in ids_datos.items()}
            nodos_estado = {c: client.get_node(f"ns={NS_IDX};s={i}") for c, i in ids_estado.items()}

            # lectura inicial: valores del esclavo y estado Modbus; luego Alcanzable; luego "enlace OK"
            for nombre, nodo in nodos.items():
                edge.dato(nombre, await nodo.read_value())
            for clave in en.CLAVES_MODBUS:
                edge.estado_modbus(clave, await nodos_estado[clave].read_value())
            edge.modbus_alcanzable(bool(await nodos_estado["Alcanzable"].read_value()))

            p = ua.CreateSubscriptionParameters()
            p.RequestedPublishingInterval = PUBLISHING_INTERVAL_MS
            p.RequestedLifetimeCount = LIFETIME_COUNT
            p.RequestedMaxKeepAliveCount = KEEPALIVE_COUNT
            p.MaxNotificationsPerPublish = 50
            p.PublishingEnabled = True
            p.Priority = 0
            sub = await client.create_subscription(p, ManejadorSuscripcion(edge, por_id))
            await sub.subscribe_data_change(list(nodos.values()) + list(nodos_estado.values()),
                                            queuesize=QUEUE_SIZE)
            while not cola.empty():       # comandos viejos de antes de la caida: se descartan
                cola.get_nowait()
            estado.opc_conectado = True
            edge.opc_conectado(True)      # recien aca se puede publicar DBIRTH con valores frescos
            espera = 1.0
            _log.info("Sesion OPC UA establecida; %d variables + estado suscriptos", len(nodos))
            await correr_juntas(vigilante(nodos_estado["Alcanzable"]), escritor(cola, edge, nodos, estado))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _log.warning("Enlace OPC UA perdido: %s: %s", type(e).__name__, e,
                         exc_info=_log.isEnabledFor(logging.DEBUG))
        finally:
            if estado.opc_conectado:
                estado.opc_reconexiones += 1
            estado.opc_conectado = False
            edge.opc_conectado(False)     # NDATA OPCUA=ERROR y DDEATH del dispositivo
            try:
                await asyncio.wait_for(client.disconnect(), timeout=3)
            except Exception:
                pass
        _log.info("Reintentando OPC UA en %.0f s", espera)
        await asyncio.sleep(espera)
        espera = min(espera * 2, ESPERA_MAX_S)


# ================================================================ G3: MQTT
def crear_cliente_mqtt(cfg: Dict[str, Any]):
    """Un cliente nuevo por sesion (asi cada conexion lleva su propio Last Will con su bdSeq)."""
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=cfg.get("client_id", ""),
                    protocol=mqtt.MQTTv311, reconnect_on_failure=False)
    if cfg.get("usuario"):
        c.username_pw_set(cfg["usuario"], cfg.get("clave") or "")
    if cfg.get("tls", True):
        c.tls_set(ca_certs=cfg.get("ca_cert") or None, cert_reqs=ssl.CERT_REQUIRED)
    return c


async def _esperar_alguno(eventos, timeout: Optional[float] = None):
    """Espera hasta que se active alguno de los asyncio.Event (o venza el timeout)."""
    tareas = [asyncio.create_task(e.wait()) for e in eventos]
    try:
        await asyncio.wait(tareas, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for t in tareas:
            t.cancel()


async def sesion_mqtt(cfg: Dict[str, Any], edge: en.NodoBorde, cola: asyncio.Queue, estado: Estado,
                      fabrica=crear_cliente_mqtt):
    """G3: ciclo de conexiones MQTT. Cada intento: bdSeq nuevo + Will -> CONNECT -> subscribe -> NBIRTH.
    Al cancelarse (Ctrl+C) hace el cierre ordenado: DDEATH + NDEATH explicitos y DISCONNECT."""
    loop = asyncio.get_running_loop()
    espera = 1.0
    actual: Dict[str, Any] = {"cliente": None}

    def publicar(topico, carga, qos, retain):
        c = actual["cliente"]
        if c is not None and estado.mqtt_conectado:
            c.publish(topico, carga, qos=qos, retain=retain)

    edge.conectar_publicador(publicar)             # el nodo publica por la sesion vigente
    while True:
        topico_will, carga_will, qos_will, retain_will = edge.nueva_sesion()
        cliente = fabrica(cfg)
        actual["cliente"] = cliente
        conectado, perdido = asyncio.Event(), asyncio.Event()

        def on_connect(c, userdata, flags, reason_code, properties=None):
            if getattr(reason_code, "is_failure", False):
                _log.warning("El broker rechazo la conexion: %s", reason_code)
                loop.call_soon_threadsafe(perdido.set)
            else:
                loop.call_soon_threadsafe(conectado.set)

        def on_disconnect(c, userdata, disconnect_flags=None, reason_code=None, properties=None):
            loop.call_soon_threadsafe(perdido.set)

        def on_message(c, userdata, msg):
            loop.call_soon_threadsafe(_rx, msg.topic, bytes(msg.payload))

        def _rx(topico, carga):
            estado.mqtt_recibidos += 1
            for accion, nombre, valor in edge.al_mensaje(topico, carga):
                if accion == "escribir":
                    cola.put_nowait((nombre, valor))

        cliente.on_connect, cliente.on_disconnect, cliente.on_message = on_connect, on_disconnect, on_message
        cliente.will_set(topico_will, carga_will, qos=qos_will, retain=retain_will)
        try:
            _log.info("Conectando a %s:%d (TLS=%s, bdSeq=%d) ...", cfg["broker"], cfg["puerto"],
                      cfg.get("tls", True), edge.bd_seq)
            await asyncio.to_thread(cliente.connect, cfg["broker"], int(cfg["puerto"]),
                                    int(cfg.get("keepalive_s", 15)))
            cliente.loop_start()
            await _esperar_alguno([conectado, perdido], timeout=10)
            if not conectado.is_set():
                raise ConnectionError("el broker no acepto la conexion")
            estado.mqtt_conectado = True
            cliente.subscribe([(t, 1) for t in edge.topicos_suscripcion()])
            edge.al_conectar()                     # NBIRTH (seq 0) + DBIRTH
            espera = 1.0
            _log.info("MQTT conectado; Edge Node nacido (bdSeq=%d)", edge.bd_seq)
            await perdido.wait()
            _log.warning("Conexion MQTT perdida")
        except asyncio.CancelledError:
            edge.cerrar()                          # DDEATH + NDEATH (el Will solo sale ante cortes)
            await asyncio.sleep(0.5)               # deja que paho vacie la cola de salida
            raise
        except Exception as e:
            estado.mqtt_intentos_fallidos += 1
            _log.warning("MQTT: %s: %s", type(e).__name__, e)
        finally:
            estado.mqtt_conectado = False
            edge.al_desconectar()
            try:
                cliente.loop_stop()
                cliente.disconnect()
            except Exception:
                pass
        _log.info("Reintentando MQTT en %.0f s (nueva sesion con bdSeq incrementado)", espera)
        await asyncio.sleep(espera)
        espera = min(espera * 2, ESPERA_MAX_S)


# ================================================================ G5: reloj del nodo y consola
async def reloj_nodo(edge: en.NodoBorde):
    while True:
        edge.tick()
        await asyncio.sleep(0.25)


def linea_estado(edge: en.NodoBorde, estado: Estado) -> str:
    v = edge.nodo_valores
    s = edge.estadisticas()
    m = s["mensajes"]
    return (f"MODBUS {v['Status/MODBUS/Estado'] or '-'} OK={v['Status/MODBUS/OK']} "
            f"CRC={v['Status/MODBUS/CRC_ERROR']} TO={v['Status/MODBUS/TIMEOUT']} | "
            f"OPCUA {v['Status/OPCUA/Estado']} esc.ok={estado.opc_escrituras_ok} "
            f"esc.err={estado.opc_escrituras_error} reconex={estado.opc_reconexiones} | "
            f"MQTT {'CONECTADO' if estado.mqtt_conectado else 'DESCONECTADO'} "
            f"reconex={v['Status/MQTT/Reconexiones']} rx={estado.mqtt_recibidos} | "
            f"SPB bdSeq={s['bdSeq']} seq={s['seq']} rebirths={s['rebirths']} "
            f"NBIRTH={m['NBIRTH']} DBIRTH={m['DBIRTH']} NDATA={m['NDATA']} DDATA={m['DDATA']} "
            f"DDEATH={m['DDEATH']} dispositivo={'EN LINEA' if s['dispositivo_en_linea'] else 'FUERA'}")


async def consola(edge: en.NodoBorde, estado: Estado, periodo_s: float = 1.0):
    while True:
        print(datetime.now().strftime("%H:%M:%S"), linea_estado(edge, estado), flush=True)
        await asyncio.sleep(periodo_s)


# ================================================================ G1: configuracion y arranque
def cargar_config(args) -> Dict[str, Any]:
    cfg = json.loads(Path(args.config).read_text(encoding="utf-8")) if Path(args.config).is_file() else {}
    cfg.setdefault("opcua", {})
    cfg.setdefault("mqtt", {})
    cfg.setdefault("sparkplug", {})
    cfg.setdefault("config_esclavo", args.config_esclavo or "config_esclavo.json")
    cfg["opcua"] = {"url": "opc.tcp://192.168.10.1:4840", "seguridad": "none", "pki": "pki",
                    "usuario": None, "clave": None, **cfg["opcua"]}
    cfg["mqtt"] = {"puerto": 8883, "tls": True, "ca_cert": None, "keepalive_s": 15, **cfg["mqtt"]}
    cfg["sparkplug"] = {"grupo": "UNRaf_PdCI", "nodo": "Gateway_OPCUA", "periodo_estado_s": 1.0,
                        "limites": {}, **cfg["sparkplug"]}
    for arg, destino, clave in ((args.url, "opcua", "url"), (args.seguridad, "opcua", "seguridad"),
                                (args.pki, "opcua", "pki"), (args.cert_servidor, "opcua", "cert_servidor"),
                                (args.usuario, "opcua", "usuario"), (args.clave, "opcua", "clave"),
                                (args.broker, "mqtt", "broker"), (args.puerto_mqtt, "mqtt", "puerto"),
                                (args.ca_cert, "mqtt", "ca_cert"), (args.mqtt_usuario, "mqtt", "usuario"),
                                (args.mqtt_clave, "mqtt", "clave"), (args.grupo, "sparkplug", "grupo"),
                                (args.nodo, "sparkplug", "nodo")):
        if arg is not None:
            cfg[destino][clave] = arg
    if args.sin_tls:
        cfg["mqtt"]["tls"] = False
    if args.config_esclavo:
        cfg["config_esclavo"] = args.config_esclavo
    if os.environ.get("MQTT_CLAVE") and args.mqtt_clave is None:
        cfg["mqtt"]["clave"] = os.environ["MQTT_CLAVE"]
    if not cfg["mqtt"].get("broker") or "COMPLETAR" in str(cfg["mqtt"].get("broker")):
        raise ValueError("falta el broker MQTT (config_gateway.json o --broker)")
    return cfg


async def main():
    ap = argparse.ArgumentParser(description="Gateway OPC UA -> MQTT Sparkplug B (TP3 PdCI)")
    ap.add_argument("--config", default="config_gateway.json")
    ap.add_argument("--config-esclavo", default=None, help="config_esclavo.json (copia del de la Raspberry)")
    ap.add_argument("--url", help="URL del servidor OPC UA")
    ap.add_argument("--seguridad", choices=("none", "sign", "encrypt"))
    ap.add_argument("--pki")
    ap.add_argument("--cert-servidor")
    ap.add_argument("--usuario", help="usuario OPC UA")
    ap.add_argument("--clave", help="contrasena OPC UA")
    ap.add_argument("--broker")
    ap.add_argument("--puerto-mqtt", type=int)
    ap.add_argument("--sin-tls", action="store_true")
    ap.add_argument("--ca-cert", help="certificado de la CA del broker (si no es de una CA publica)")
    ap.add_argument("--mqtt-usuario")
    ap.add_argument("--mqtt-clave")
    ap.add_argument("--grupo")
    ap.add_argument("--nodo")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("asyncua").setLevel(logging.WARNING)
    if args.debug:
        logging.getLogger("edge").setLevel(logging.DEBUG)
        logging.getLogger("gateway").setLevel(logging.DEBUG)   # muestra el traceback de los errores OPC UA
    try:
        cfg = cargar_config(args)
        dispositivo, variables = en.cargar_config_esclavo(cfg["config_esclavo"])
    except (OSError, ValueError, KeyError) as e:
        print(f"Error de configuracion: {e}", file=sys.stderr)
        return 2

    sp_cfg = cfg["sparkplug"]
    limites = {k: tuple(v) for k, v in sp_cfg["limites"].items()}
    edge = en.NodoBorde(sp_cfg["grupo"], sp_cfg["nodo"], dispositivo, variables,
                        lambda *a: None, limites, float(sp_cfg["periodo_estado_s"]))
    estado, cola = Estado(), asyncio.Queue()
    mqtt_cfg = {**cfg["mqtt"], "client_id": sp_cfg["nodo"]}
    tareas = [asyncio.create_task(c) for c in (
        sesion_opcua(cfg["opcua"], dispositivo, edge, cola, estado),
        sesion_mqtt(mqtt_cfg, edge, cola, estado),
        reloj_nodo(edge), consola(edge, estado))]
    try:
        await asyncio.gather(*tareas)
    finally:
        for t in tareas:
            t.cancel()
        await asyncio.gather(*tareas, return_exceptions=True)   # deja terminar el cierre ordenado
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        pass
