#!/usr/bin/env python3
"""
Maestro MODBUS RTU (dispositivo 2) - TP3 Integradora | Protocolos de Comunicaciones Industriales | UNRaf

Implementa el maestro "a mano" (sin librerias Modbus): arma la trama, calcula y verifica el
CRC-16, lee la respuesta por el puerto serie RS485 y lleva las estadisticas de cada peticion.

Bloques (para trazabilidad en el informe):
  M1 - Configuracion (archivo JSON + argumentos de linea de comandos)
  M2 - Capa de trama: crc16() y armado/validacion de tramas
  M3 - ModbusMaestro: una transaccion = envio de la solicitud + recepcion y validacion de la respuesta
  M4 - Contadores de peticiones (OK / CRC_ERROR / TIMEOUT) y estado de la ultima peticion
  M5 - Poller: ciclo de lectura de la tabla de variables, estado de comunicacion, escrituras
  M6 - Consola: una linea por ciclo

Estados de comunicacion (nombres exactos pedidos por la consigna):
  "DONE"                -> la respuesta llego, el CRC es correcto y la trama es valida
  "CRC ERROR"           -> llego algo, pero no se pudo recomponer la trama o el CRC no coincide
  "DEVICE NOT REACHED"  -> no llego ningun byte antes del timeout

Contadores (consigna 1.c):
  ok         respuestas validas (incluye respuestas de excepcion Modbus, que son tramas validas)
  crc_error  respuestas con CRC invalido, incompletas, de otro esclavo o con formato inesperado
  timeout    peticiones sin ninguna respuesta
  excepciones (aparte) cuantas de las respuestas validas fueron de excepcion Modbus

Requisitos: Python 3.8+, pyserial
Uso:
  python maestro_modbus.py                                   (usa config_esclavo.json)
  python maestro_modbus.py --puerto /dev/ttyUSB0 --baudrate 19200 --paridad E --id 5
  python maestro_modbus.py --una-vez --escribir Outputs/LED_PWM=128 --escribir Outputs/LED_Digital=1
  python maestro_modbus.py --debug                           (muestra cada trama TX/RX en hexadecimal)
"""
from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import logging
import struct
import sys
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import serial

_log = logging.getLogger("modbus")

ESTADO_OK = "DONE"
ESTADO_CRC = "CRC ERROR"
ESTADO_SIN_RESPUESTA = "DEVICE NOT REACHED"

# Area de datos -> funciones Modbus (lectura / escritura) y si es de bits o de registros de 16 bits
AREAS = {
    "coil":             {"fc_lectura": 0x01, "fc_escritura": 0x05, "bits": True},
    "discrete_input":   {"fc_lectura": 0x02, "fc_escritura": None, "bits": True},
    "holding_register": {"fc_lectura": 0x03, "fc_escritura": 0x06, "bits": False},
    "input_register":   {"fc_lectura": 0x04, "fc_escritura": None, "bits": False},
}
TIPOS_BITS = ("Boolean",)
TIPOS_REGISTRO = ("UInt16", "Int16")

NOMBRES_EXCEPCION = {
    0x01: "ILLEGAL FUNCTION",
    0x02: "ILLEGAL DATA ADDRESS",
    0x03: "ILLEGAL DATA VALUE",
    0x04: "SERVER DEVICE FAILURE",
}


# ================================================================ M2: capa de trama
def crc16(datos: bytes) -> int:
    """CRC-16 MODBUS: polinomio 0xA001 (0x8005 reflejado), valor inicial 0xFFFF.
    Por cada byte: XOR con el CRC y 8 desplazamientos a la derecha; si el bit que sale
    es 1 se hace XOR con 0xA001. En la trama viaja con el byte BAJO primero."""
    crc = 0xFFFF
    for byte in datos:
        crc ^= byte
        for _ in range(8):
            if crc & 0x0001:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc


def _hex(datos: bytes) -> str:
    return " ".join(f"{b:02X}" for b in datos)


class ErrorModbus(Exception):
    """Base de los errores de una peticion. 'estado' es el texto de estado asociado."""
    estado = ESTADO_CRC


class SinRespuesta(ErrorModbus):
    estado = ESTADO_SIN_RESPUESTA


class TramaInvalida(ErrorModbus):
    estado = ESTADO_CRC


class ExcepcionModbus(ErrorModbus):
    """El esclavo respondio con una trama valida de excepcion (FC | 0x80 + codigo)."""
    estado = ESTADO_OK

    def __init__(self, fc: int, codigo: int):
        self.fc = fc
        self.codigo = codigo
        nombre = NOMBRES_EXCEPCION.get(codigo, "DESCONOCIDA")
        super().__init__(f"excepcion Modbus 0x{codigo:02X} ({nombre}) a la funcion 0x{fc:02X}")


# ================================================================ M4: contadores
@dataclass
class Contadores:
    ok: int = 0
    crc_error: int = 0
    timeout: int = 0
    excepciones: int = 0

    def como_dict(self) -> Dict[str, int]:
        return asdict(self)


# ================================================================ M3: maestro Modbus RTU
class ModbusMaestro:
    """Maestro Modbus RTU sobre un puerto serie RS485 (adaptador USB-RS485 con control de
    direccion automatico). Toda comunicacion pasa por _transaccion(), que actualiza los
    contadores. Es seguro usarlo desde varios hilos (un candado por transaccion)."""

    def __init__(self, puerto: str, baudrate: int = 9600, paridad: str = "N",
                 bits_parada: int = 1, timeout_s: float = 0.3):
        self.puerto = puerto
        self.baudrate = baudrate
        self.paridad = paridad
        self.bits_parada = bits_parada
        self.timeout_s = timeout_s
        # Bits por caracter en el cable: inicio + 8 datos + paridad (si hay) + parada
        bits = 1 + 8 + (0 if paridad == "N" else 1) + bits_parada
        self.t_caracter = bits / baudrate
        # Silencio entre tramas: 3,5 caracteres (fijo en 1,75 ms por encima de 19200 bps)
        self.t35 = 0.00175 if baudrate > 19200 else 3.5 * self.t_caracter
        self.contadores = Contadores()
        self.ultimo_estado: Optional[str] = None
        self.ultimo_error: str = ""
        self._serie: Optional[serial.Serial] = None
        self._lock = threading.RLock()
        self._t_fin = 0.0                       # instante de la ultima actividad en el bus

    # ---------------------------------------------------------------- puerto
    def abrir(self):
        if self._serie is not None and self._serie.is_open:
            return
        self._serie = serial.Serial(self.puerto, self.baudrate, bytesize=8,
                                    parity=self.paridad, stopbits=self.bits_parada,
                                    timeout=self.timeout_s, write_timeout=1.0)

    def cerrar(self):
        try:
            if self._serie is not None:
                self._serie.close()
        except Exception:
            pass
        self._serie = None

    def _esperar_silencio(self):
        """Respeta el silencio de t3,5 entre el fin de la transaccion anterior y la nueva trama."""
        falta = self.t35 - (time.monotonic() - self._t_fin)
        if falta > 0:
            time.sleep(falta)

    def _leer(self, cantidad: int, timeout: float) -> bytes:
        self._serie.timeout = timeout
        return self._serie.read(cantidad)

    def _vaciar(self):
        """Descarta lo que siga llegando hasta que el bus quede en silencio."""
        self._serie.timeout = max(2 * self.t35, 0.01)
        while self._serie.read(64):
            pass

    # ---------------------------------------------------------------- una transaccion
    def _recibir(self, esclavo: int, fc_pedido: int) -> bytes:
        """Lee y valida la respuesta. El largo se deduce de los primeros 3 bytes:
          excepcion (FC>=0x80)       : 5 bytes  [ID][FC|80][codigo][CRC lo][CRC hi]
          lectura (FC 01..04)        : 5 + byte_count   [ID][FC][n][datos...][CRC]
          escritura simple (FC 05/06): 8 bytes  [ID][FC][dir 2][valor 2][CRC]"""
        cabecera = self._leer(3, self.timeout_s)
        if len(cabecera) == 0:
            raise SinRespuesta(f"sin respuesta en {self.timeout_s * 1000:.0f} ms")
        if len(cabecera) < 3:
            self._vaciar()
            raise TramaInvalida(f"respuesta incompleta ({len(cabecera)} bytes)")

        fc = cabecera[1]
        if fc & 0x80:
            total = 5
        elif fc in (0x01, 0x02, 0x03, 0x04):
            total = 5 + cabecera[2]
        elif fc in (0x05, 0x06):
            total = 8
        else:
            self._vaciar()
            raise TramaInvalida(f"codigo de funcion 0x{fc:02X} desconocido en la respuesta")

        faltan = total - 3
        margen = 0.05 + faltan * self.t_caracter        # holgura por latencia del adaptador USB
        resto = self._leer(faltan, margen)
        trama = cabecera + resto
        _log.debug("RX %s", _hex(trama))
        if len(trama) < total:
            raise TramaInvalida(f"respuesta incompleta ({len(trama)} de {total} bytes)")

        crc_rx = trama[-2] | (trama[-1] << 8)           # byte bajo primero
        crc_calc = crc16(trama[:-2])
        if crc_rx != crc_calc:
            raise TramaInvalida(f"CRC invalido (recibido 0x{crc_rx:04X}, calculado 0x{crc_calc:04X})")
        if trama[0] != esclavo:
            raise TramaInvalida(f"respuesta del esclavo {trama[0]} (se esperaba {esclavo})")
        if (fc & 0x7F) != fc_pedido:
            raise TramaInvalida(f"funcion 0x{fc:02X} inesperada (se pidio 0x{fc_pedido:02X})")
        return trama

    def _transaccion(self, esclavo: int, pdu: bytes,
                     validar: Optional[Callable[[bytes], None]] = None) -> bytes:
        """Envia [ID][PDU][CRC] y devuelve la respuesta validada. Actualiza los contadores.
        'validar' comprueba el contenido de una respuesta correcta y puede lanzar TramaInvalida."""
        fc_pedido = pdu[0]
        solicitud = bytes([esclavo]) + pdu
        solicitud += struct.pack("<H", crc16(solicitud))         # CRC byte bajo primero
        with self._lock:
            try:
                try:
                    self.abrir()
                    self._esperar_silencio()
                    self._serie.reset_input_buffer()
                    _log.debug("TX %s", _hex(solicitud))
                    self._serie.write(solicitud)
                    self._serie.flush()
                    respuesta = self._recibir(esclavo, fc_pedido)
                    if not (respuesta[1] & 0x80) and validar is not None:
                        validar(respuesta)
                except (serial.SerialException, OSError) as e:
                    self.cerrar()                                 # se reabre en la proxima peticion
                    raise SinRespuesta(f"puerto serie no disponible: {e}") from e
            except SinRespuesta as e:
                self.contadores.timeout += 1
                self._registrar(e)
                raise
            except TramaInvalida as e:
                self.contadores.crc_error += 1
                self._registrar(e)
                raise
            finally:
                self._t_fin = time.monotonic()

            self.contadores.ok += 1                               # trama valida (con o sin excepcion)
            if respuesta[1] & 0x80:
                self.contadores.excepciones += 1
                error = ExcepcionModbus(respuesta[1] & 0x7F, respuesta[2])
                self.ultimo_estado = ESTADO_OK
                self.ultimo_error = str(error)
                raise error
            self.ultimo_estado = ESTADO_OK
            self.ultimo_error = ""
            return respuesta

    def _registrar(self, error: ErrorModbus):
        self.ultimo_estado = error.estado
        self.ultimo_error = str(error)
        _log.debug("peticion fallida: %s -> %s", self.ultimo_estado, self.ultimo_error)

    # ---------------------------------------------------------------- operaciones
    def leer(self, esclavo: int, area: str, direccion: int, cantidad: int = 1) -> List[Any]:
        """FC 01/02 (bits) o FC 03/04 (registros). Devuelve lista de bool o de int sin signo."""
        info = AREAS[area]
        pdu = struct.pack(">BHH", info["fc_lectura"], direccion, cantidad)

        def validar(resp: bytes):
            esperado = (cantidad + 7) // 8 if info["bits"] else cantidad * 2
            if resp[2] != esperado:
                raise TramaInvalida(f"byte count {resp[2]} (se esperaba {esperado})")

        resp = self._transaccion(esclavo, pdu, validar)
        datos = resp[3:3 + resp[2]]
        if info["bits"]:
            return [bool(datos[i // 8] & (1 << (i % 8))) for i in range(cantidad)]
        return list(struct.unpack(f">{cantidad}H", datos))

    def escribir_coil(self, esclavo: int, direccion: int, valor: bool):
        """FC 05: valor 0xFF00 = ON, 0x0000 = OFF. La respuesta debe repetir la solicitud."""
        pdu = struct.pack(">BHH", 0x05, direccion, 0xFF00 if valor else 0x0000)
        self._transaccion(esclavo, pdu, self._validar_eco(pdu))

    def escribir_registro(self, esclavo: int, direccion: int, valor: int):
        """FC 06: escribe un registro de 16 bits. La respuesta debe repetir la solicitud."""
        pdu = struct.pack(">BHH", 0x06, direccion, valor & 0xFFFF)
        self._transaccion(esclavo, pdu, self._validar_eco(pdu))

    @staticmethod
    def _validar_eco(pdu: bytes) -> Callable[[bytes], None]:
        def validar(resp: bytes):
            if resp[1:6] != pdu:
                raise TramaInvalida(f"el eco de la escritura no coincide ({_hex(resp[1:6])})")
        return validar


# ================================================================ M1 / M5: variables y poller
@dataclass
class Variable:
    nombre: str
    tipo: str
    area: str
    direccion: int

    def __post_init__(self):
        if self.area not in AREAS:
            raise ValueError(f"{self.nombre}: area '{self.area}' desconocida {list(AREAS)}")
        es_bits = AREAS[self.area]["bits"]
        validos = TIPOS_BITS if es_bits else TIPOS_REGISTRO
        if self.tipo not in validos:
            raise ValueError(f"{self.nombre}: el area {self.area} admite los tipos {validos}")

    @property
    def escribible(self) -> bool:
        return AREAS[self.area]["fc_escritura"] is not None

    def decodificar(self, crudo) -> Any:
        if self.tipo == "Int16" and crudo >= 0x8000:
            return crudo - 0x10000
        return crudo

    def convertir(self, valor) -> Any:
        """Convierte (y valida el rango de) un valor recibido del usuario: bool/int o texto."""
        if self.tipo == "Boolean":
            if isinstance(valor, str):
                t = valor.strip().lower()
                if t in ("1", "true", "on", "si"):
                    return True
                if t in ("0", "false", "off", "no"):
                    return False
                raise ValueError(f"{self.nombre}: '{valor}' no es un booleano")
            return bool(valor)
        numero = int(valor, 0) if isinstance(valor, str) else int(valor)
        minimo, maximo = (0, 0xFFFF) if self.tipo == "UInt16" else (-0x8000, 0x7FFF)
        if not minimo <= numero <= maximo:
            raise ValueError(f"{self.nombre}: {numero} fuera de rango {minimo}..{maximo}")
        return numero


class Poller:
    """Lee periodicamente la tabla de variables del esclavo, resume el estado de la
    comunicacion y atiende las escrituras. Pensado para que lo use luego el servidor OPC UA.

    estado      : resumen del ultimo ciclo, con prioridad
                    DEVICE NOT REACHED (alguna peticion sin respuesta)
                  > CRC ERROR (alguna respuesta invalida)
                  > DONE
    alcanzable  : None (aun sin datos) / True / False. Pasa a False tras 'ciclos_para_perdido'
                  ciclos seguidos en los que NINGUNA peticion obtuvo ni un byte de respuesta (esto
                  es lo que luego se publica como DDEATH); vuelve a True con la primera respuesta.
                  Con el esclavo perdido solo se sondea la primera variable (recuperacion rapida)."""

    def __init__(self, maestro: ModbusMaestro, esclavo_id: int, variables: List[Variable],
                 ciclos_para_perdido: int = 3):
        if not variables:
            raise ValueError("la tabla de variables esta vacia")
        self.maestro = maestro
        self.esclavo_id = esclavo_id
        self.variables = variables
        self.por_nombre = {v.nombre: v for v in variables}
        self.ciclos_para_perdido = ciclos_para_perdido
        self.valores: Dict[str, Any] = {v.nombre: None for v in variables}
        self.estado: Optional[str] = None
        self.alcanzable: Optional[bool] = None
        self.ciclos = 0
        self._fallas_seguidas = 0
        # Un ciclo, una escritura y una relectura no se mezclan entre si (se pueden pedir desde hilos distintos)
        self._lock = threading.RLock()

    def ciclo(self) -> Dict[str, Any]:
        with self._lock:
            return self._ciclo()

    def _ciclo(self) -> Dict[str, Any]:
        a_leer = [self.variables[0]] if self.alcanzable is False else self.variables
        resultados: Dict[str, str] = {}
        cambios: Dict[str, Any] = {}
        hubo_ok = hubo_crc = hubo_timeout = False

        for v in a_leer:
            try:
                crudo = self.maestro.leer(self.esclavo_id, v.area, v.direccion, 1)[0]
                valor = v.decodificar(crudo)
                if self.valores[v.nombre] != valor:
                    cambios[v.nombre] = valor
                self.valores[v.nombre] = valor
                resultados[v.nombre] = "ok"
                hubo_ok = True
            except ExcepcionModbus as e:
                resultados[v.nombre] = f"excepcion 0x{e.codigo:02X}"
                hubo_ok = True                       # la trama llego bien: el esclavo esta presente
            except SinRespuesta:
                resultados[v.nombre] = "sin respuesta"
                hubo_timeout = True
            except TramaInvalida:
                resultados[v.nombre] = "trama invalida"
                hubo_crc = True

        if hubo_timeout:
            self.estado = ESTADO_SIN_RESPUESTA
        elif hubo_crc:
            self.estado = ESTADO_CRC
        else:
            self.estado = ESTADO_OK

        if hubo_ok or hubo_crc:                       # llego algun byte: el esclavo esta presente
            self._fallas_seguidas = 0
            self.alcanzable = True
        elif hubo_timeout:
            self._fallas_seguidas += 1
            if self._fallas_seguidas >= self.ciclos_para_perdido:
                self.alcanzable = False

        self.ciclos += 1
        return {"estado": self.estado, "alcanzable": self.alcanzable,
                "valores": dict(self.valores), "cambios": cambios, "resultados": resultados,
                "contadores": self.maestro.contadores.como_dict()}

    def escribir(self, nombre: str, valor) -> Any:
        """Escribe una variable de salida. Devuelve el valor ya convertido. Lanza ValueError si el
        nombre no existe / no es escribible / el valor esta fuera de rango, o ErrorModbus si falla."""
        with self._lock:
            return self._escribir(nombre, valor)

    def _escribir(self, nombre: str, valor) -> Any:
        if nombre not in self.por_nombre:
            raise ValueError(f"variable desconocida: {nombre}")
        v = self.por_nombre[nombre]
        if not v.escribible:
            raise ValueError(f"{nombre} es de solo lectura")
        valor = v.convertir(valor)
        if v.area == "coil":
            self.maestro.escribir_coil(self.esclavo_id, v.direccion, valor)
        else:
            self.maestro.escribir_registro(self.esclavo_id, v.direccion, valor)
        # No se actualiza self.valores: el estado de la salida se confirma con la relectura
        # del proximo ciclo (asi lo que se publica es lo que realmente tiene el esclavo).
        return valor

    def releer(self, nombre: str) -> Dict[str, Any]:
        """Relee UNA variable (p. ej. justo despues de escribirla, para confirmar el estado real sin
        esperar al proximo ciclo). Devuelve {nombre: valor} si el valor cambio, o {} si no.
        Lanza ErrorModbus si la lectura falla."""
        with self._lock:
            v = self.por_nombre[nombre]
            valor = v.decodificar(self.maestro.leer(self.esclavo_id, v.area, v.direccion, 1)[0])
            cambio = {nombre: valor} if self.valores[nombre] != valor else {}
            self.valores[nombre] = valor
            return cambio

    async def correr(self, intervalo_s: float, on_ciclo=None):
        """Bucle asincrono (para integrarlo al servidor OPC UA): el acceso al puerto serie se
        ejecuta en un hilo para no bloquear el bucle de eventos. Sin deriva temporal."""
        loop = asyncio.get_running_loop()
        proximo = loop.time()
        while True:
            resultado = await asyncio.to_thread(self.ciclo)
            if on_ciclo is not None:
                r = on_ciclo(resultado)
                if inspect.isawaitable(r):
                    await r
            proximo += intervalo_s
            await asyncio.sleep(max(0.0, proximo - loop.time()))


# ================================================================ M1: configuracion
CONFIG_DEFECTO = {
    "serie": {"puerto": "/dev/ttyUSB0", "baudrate": 9600, "paridad": "N",
              "bits_parada": 1, "timeout_ms": 300},
    "esclavo_id": 1,
    "polling_ms": 500,
    "ciclos_para_perdido": 3,
    "variables": [],
}


def cargar_config(ruta: Optional[str], args: Optional[argparse.Namespace] = None) -> Dict[str, Any]:
    """Lee el JSON (si existe) y aplica encima lo indicado por linea de comandos."""
    cfg = json.loads(json.dumps(CONFIG_DEFECTO))
    if ruta and Path(ruta).is_file():
        leido = json.loads(Path(ruta).read_text(encoding="utf-8"))
        for clave, valor in leido.items():
            if isinstance(valor, dict) and isinstance(cfg.get(clave), dict):
                cfg[clave].update(valor)
            else:
                cfg[clave] = valor
    elif ruta:
        raise FileNotFoundError(f"no existe el archivo de configuracion {ruta}")
    if args is not None:
        for destino, valor in (("puerto", args.puerto), ("baudrate", args.baudrate),
                               ("paridad", args.paridad), ("bits_parada", args.bits_parada),
                               ("timeout_ms", args.timeout_ms)):
            if valor is not None:
                cfg["serie"][destino] = valor
        if args.id is not None:
            cfg["esclavo_id"] = args.id
        if args.polling_ms is not None:
            cfg["polling_ms"] = args.polling_ms
    return cfg


def crear_desde_config(cfg: Dict[str, Any]):
    s = cfg["serie"]
    maestro = ModbusMaestro(s["puerto"], int(s["baudrate"]), s["paridad"],
                            int(s["bits_parada"]), int(s["timeout_ms"]) / 1000.0)
    variables = [Variable(**v) for v in cfg["variables"]]
    poller = Poller(maestro, int(cfg["esclavo_id"]), variables, int(cfg["ciclos_para_perdido"]))
    return maestro, poller


# ================================================================ M6: consola
def linea_consola(res: Dict[str, Any]) -> str:
    c = res["contadores"]
    valores = " ".join(f"{n}={'-' if v is None else int(v) if isinstance(v, bool) else v}"
                       for n, v in res["valores"].items())
    return (f"{time.strftime('%H:%M:%S')} {res['estado']:<18} | {valores} | "
            f"OK={c['ok']} CRC_ERROR={c['crc_error']} TIMEOUT={c['timeout']} EXC={c['excepciones']}")


def main():
    ap = argparse.ArgumentParser(description="Maestro MODBUS RTU (TP3 PdCI)")
    ap.add_argument("--config", default="config_esclavo.json")
    ap.add_argument("--puerto")
    ap.add_argument("--baudrate", type=int)
    ap.add_argument("--paridad", choices=("N", "E", "O"))
    ap.add_argument("--bits-parada", type=int, choices=(1, 2))
    ap.add_argument("--id", type=int, help="ID Modbus del esclavo (1..247)")
    ap.add_argument("--timeout-ms", type=int)
    ap.add_argument("--polling-ms", type=int)
    ap.add_argument("--una-vez", action="store_true", help="un solo ciclo y termina")
    ap.add_argument("--escribir", action="append", default=[], metavar="NOMBRE=VALOR",
                    help="escribe una salida antes de empezar (se puede repetir)")
    ap.add_argument("--debug", action="store_true", help="muestra cada trama TX/RX en hexadecimal")
    args = ap.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        cfg = cargar_config(args.config, args)
        maestro, poller = crear_desde_config(cfg)
    except (OSError, ValueError, KeyError, TypeError) as e:
        print(f"Error de configuracion: {e}", file=sys.stderr)
        return 2

    _log.info("Maestro Modbus: %s, %d bps, 8%s%d, esclavo ID %d, %d variables",
              maestro.puerto, maestro.baudrate, maestro.paridad, maestro.bits_parada,
              poller.esclavo_id, len(poller.variables))

    for texto in args.escribir:
        nombre, _, valor = texto.partition("=")
        try:
            aplicado = poller.escribir(nombre.strip(), valor.strip())
            print(f"Escrito {nombre.strip()} = {aplicado}")
        except ErrorModbus as e:
            print(f"No se pudo escribir {nombre.strip()}: {e}")
        except ValueError as e:
            print(f"Escritura rechazada: {e}")

    intervalo = int(cfg["polling_ms"]) / 1000.0
    proximo = time.monotonic()
    try:
        while True:
            print(linea_consola(poller.ciclo()), flush=True)
            if args.una_vez:
                break
            proximo += intervalo
            time.sleep(max(0.0, proximo - time.monotonic()))
    except KeyboardInterrupt:
        pass
    finally:
        maestro.cerrar()
    return 0


if __name__ == "__main__":
    sys.exit(main())
