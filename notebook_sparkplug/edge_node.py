"""
Edge Node Sparkplug B (dispositivo 3) - TP3 Integradora | PdCI | UNRaf

Esta capa NO depende de MQTT ni de OPC UA: recibe eventos ("el nodo X cambio", "se conecto el broker",
"llego un mensaje") y publica por medio de una funcion `publicar(topico, payload, qos, retain)`.
Asi la logica del ciclo de vida de Sparkplug B se puede probar sin broker ni servidor.

Roles: este programa es el Edge Node `Gateway_OPCUA`; el esclavo Modbus (dispositivo 1) es un Device
del nodo; la pagina web es la Host Application.

Ciclo de vida (consigna 3 y 4):
  nueva_sesion()    bdSeq += 1 y arma el NDEATH (Last Will) de la conexion MQTT que se va a abrir
  al_conectar()     NBIRTH (seq = 0; bdSeq, Node Control/Rebirth, Status/*) y, si el esclavo esta vivo, DBIRTH
  dato(...)         DDATA solo ante un cambio de valor
  estado_*(...)     NDATA con las metricas de estado (Estado de inmediato; contadores cada periodo_estado_s)
  modbus_alcanzable(False) -> DDEATH ; (True) -> nuevo DBIRTH.  OPC UA caido -> DDEATH tambien.
  al_mensaje(...)   NCMD Node Control/Rebirth -> nuevo NBIRTH (seq = 0, mismo bdSeq) + DBIRTH
                    DCMD -> devuelve las escrituras a hacer sobre OPC UA (el nodo no se escribe aca)
  cerrar()          cierre ordenado: DDEATH + NDEATH explicito (el Will solo se envia ante cortes)

`seq` cuenta 0..255 y se incrementa en cada mensaje que publica el nodo (NBIRTH, DBIRTH, NDATA, DDATA,
DDEATH). El NDEATH no lleva seq, solo la metrica bdSeq.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import sparkplug_b as sp

_log = logging.getLogger("edge")

REBIRTH = "Node Control/Rebirth"
BDSEQ = "bdSeq"
# Metricas de estado del Edge Node: nombre -> (tipo, valor inicial, se publica de inmediato al cambiar)
METRICAS_NODO = {
    "Status/MODBUS/Estado": ("String", None, True),
    "Status/MODBUS/OK": ("UInt32", None, False),
    "Status/MODBUS/CRC_ERROR": ("UInt32", None, False),
    "Status/MODBUS/TIMEOUT": ("UInt32", None, False),
    "Status/OPCUA/Estado": ("String", "ERROR", True),
    "Status/MQTT/Reconexiones": ("UInt32", 0, False),
    "Status/Sparkplug/Rebirths": ("UInt32", 0, False),
}
CLAVES_MODBUS = ("Estado", "OK", "CRC_ERROR", "TIMEOUT")

QOS_DATOS = 0       # Sparkplug B: BIRTH / DATA / DDEATH con QoS 0
QOS_NDEATH = 1      # y NDEATH (Will) con QoS 1; ninguno se retiene


@dataclass
class Variable:
    nombre: str          # metrica Sparkplug, ej. "Outputs/LED_PWM"
    tipo: str            # "Boolean", "UInt16", ...
    escribible: bool


def cargar_config_esclavo(ruta: str) -> Tuple[str, List[Variable]]:
    """Lee el mismo JSON que usa el maestro Modbus (config_esclavo.json): id del esclavo y variables.
    Una variable es escribible si esta en una bobina (coil) o en un holding register."""
    cfg = json.loads(Path(ruta).read_text(encoding="utf-8"))
    variables = [Variable(v["nombre"], v["tipo"], v["area"] in ("coil", "holding_register"))
                 for v in cfg["variables"]]
    for v in variables:
        if v.tipo not in sp.TIPOS:
            raise ValueError(f"{v.nombre}: tipo {v.tipo} no soportado por Sparkplug B")
    return f"Esclavo_Modbus_ID{int(cfg['esclavo_id'])}", variables


def _rango_tipo(tipo: str) -> Tuple[int, int]:
    bits = {"Int8": 8, "Int16": 16, "Int32": 32, "UInt8": 8, "UInt16": 16, "UInt32": 32}[tipo]
    if tipo.startswith("U"):
        return 0, (1 << bits) - 1
    return -(1 << (bits - 1)), (1 << (bits - 1)) - 1


class NodoBorde:
    def __init__(self, grupo: str, nodo: str, dispositivo: str, variables: List[Variable],
                 publicar: Callable[[str, bytes, int, bool], None],
                 limites: Optional[Dict[str, Tuple[int, int]]] = None,
                 periodo_estado_s: float = 1.0,
                 reloj_ms: Callable[[], int] = lambda: int(time.time() * 1000),
                 reloj_s: Callable[[], float] = time.monotonic):
        self.grupo, self.nodo, self.dispositivo = grupo, nodo, dispositivo
        self.variables = {v.nombre: v for v in variables}
        self._orden = [v.nombre for v in variables]
        self._publicar = publicar
        self.limites = dict(limites or {})
        self.periodo_estado_s = periodo_estado_s
        self._ms, self._s = reloj_ms, reloj_s

        self.bd_seq = -1                  # se incrementa al abrir cada sesion MQTT
        self.seq = 0
        self.sesiones = 0                 # intentos de conexion (cada uno lleva su bdSeq)
        self.conectadas = 0               # CONNACK recibidos
        self.nacido = False               # NBIRTH publicado en la sesion actual
        self.disp_nacido = False          # DBIRTH publicado y sin DDEATH posterior
        self.opc_ok = False
        self.alcanzable: Optional[bool] = None       # Status.MODBUS.Alcanzable (None = desconocido)
        self.valores: Dict[str, Any] = {}             # ultimo valor conocido de cada variable
        self._ts: Dict[str, int] = {}
        self._publicado: Dict[str, Any] = {}          # lo que ya vio el Host en DBIRTH/DDATA
        self.nodo_valores = {k: v[1] for k, v in METRICAS_NODO.items()}
        self._nodo_ts: Dict[str, int] = {}
        self._pendientes: List[str] = []              # metricas de nodo a enviar en el proximo NDATA
        self._ultimo_flush = self._s()
        self.estadisticas_msg: Dict[str, int] = {m: 0 for m in sp.MENSAJES}
        self.comandos_rechazados = 0

    def conectar_publicador(self, publicar: Callable[[str, bytes, int, bool], None]):
        """Cambia la funcion de salida (cada sesion MQTT publica por su propio cliente)."""
        self._publicar = publicar

    # ------------------------------------------------------------ topicos
    def _t(self, mensaje: str, con_dispositivo: bool = False) -> str:
        return sp.topico(self.grupo, mensaje, self.nodo, self.dispositivo if con_dispositivo else None)

    def topicos_suscripcion(self) -> List[str]:
        return [self._t("NCMD"), self._t("DCMD", True)]

    # ------------------------------------------------------------ envio
    def _enviar(self, mensaje: str, metricas: List[Dict[str, Any]], con_dispositivo: bool = False):
        carga = sp.codificar(metricas, seq=self.seq, timestamp_ms=self._ms())
        self._publicar(self._t(mensaje, con_dispositivo), carga, QOS_DATOS, False)
        self.estadisticas_msg[mensaje] += 1
        _log.info("%s seq=%d (%d metricas)", mensaje, self.seq, len(metricas))
        self.seq = (self.seq + 1) % 256

    def _metrica_nodo(self, nombre: str) -> Dict[str, Any]:
        return sp.metrica(nombre, METRICAS_NODO[nombre][0], self.nodo_valores[nombre],
                          self._nodo_ts.get(nombre, self._ms()))

    def _metrica_disp(self, nombre: str) -> Dict[str, Any]:
        return sp.metrica(nombre, self.variables[nombre].tipo, self.valores.get(nombre),
                          self._ts.get(nombre, self._ms()))

    # ------------------------------------------------------------ sesion MQTT
    def nueva_sesion(self) -> Tuple[str, bytes, int, bool]:
        """Llamar ANTES de abrir cada conexion MQTT. Devuelve (topico, payload, qos, retain) del NDEATH
        que se registra como Last Will, con el bdSeq de esta sesion."""
        self.bd_seq = (self.bd_seq + 1) % 256
        self.sesiones += 1
        self.nacido = False
        self.disp_nacido = False
        self._pendientes.clear()
        ahora = self._ms()
        carga = sp.codificar([sp.metrica(BDSEQ, "Int64", self.bd_seq, ahora)], timestamp_ms=ahora)
        return self._t("NDEATH"), carga, QOS_NDEATH, False

    def al_conectar(self):
        """CONNACK recibido: publica NBIRTH (seq = 0) y DBIRTH si el esclavo esta vivo."""
        if self.conectadas > 0:           # no es la primera conexion: cuenta como reconexion
            self._set_nodo("Status/MQTT/Reconexiones", self.nodo_valores["Status/MQTT/Reconexiones"] + 1)
        self.conectadas += 1
        self._nbirth()

    def al_desconectar(self):
        self.nacido = False
        self.disp_nacido = False

    def _nbirth(self):
        self.seq = 0
        self._pendientes.clear()
        metricas = [sp.metrica(BDSEQ, "Int64", self.bd_seq, self._ms()),
                    sp.metrica(REBIRTH, "Boolean", False, self._ms())]
        metricas += [self._metrica_nodo(n) for n in METRICAS_NODO]
        self._enviar("NBIRTH", metricas)
        self.nacido = True
        self.disp_nacido = False
        self._evaluar()

    # ------------------------------------------------------------ ciclo de vida del dispositivo
    def _vivo(self) -> bool:
        return (self.opc_ok and self.alcanzable is True
                and all(self.valores.get(n) is not None for n in self._orden))

    def _evaluar(self):
        """Decide si hace falta DBIRTH o DDEATH segun el estado del enlace OPC UA y del esclavo."""
        if not self.nacido:
            return
        vivo = self._vivo()
        if vivo and not self.disp_nacido:
            self._enviar("DBIRTH", [self._metrica_disp(n) for n in self._orden], True)
            self._publicado = dict(self.valores)
            self.disp_nacido = True
        elif not vivo and self.disp_nacido:
            self._enviar("DDEATH", [], True)
            self.disp_nacido = False

    # ------------------------------------------------------------ entradas desde OPC UA
    def opc_conectado(self, ok: bool):
        self.opc_ok = ok
        if not ok:
            self.alcanzable = None        # se vuelve a leer al reconectar
        self._set_nodo("Status/OPCUA/Estado", "DONE" if ok else "ERROR")
        self._evaluar()

    def modbus_alcanzable(self, alcanzable: bool):
        self.alcanzable = bool(alcanzable)
        self._evaluar()

    def estado_modbus(self, clave: str, valor: Any):
        if clave in CLAVES_MODBUS:
            self._set_nodo(f"Status/MODBUS/{clave}", valor)

    def dato(self, nombre: str, valor: Any, ts_ms: Optional[int] = None):
        v = self.variables.get(nombre)
        if v is None or valor is None:
            return
        valor = self._coercion(v.tipo, valor)
        self.valores[nombre] = valor
        self._ts[nombre] = ts_ms if ts_ms is not None else self._ms()
        if self.disp_nacido:
            if valor != self._publicado.get(nombre):
                self._enviar("DDATA", [self._metrica_disp(nombre)], True)
                self._publicado[nombre] = valor
        else:
            self._evaluar()

    @staticmethod
    def _coercion(tipo: str, valor: Any) -> Any:
        if tipo == "Boolean":
            return bool(valor)
        if tipo == "String":
            return str(valor)
        if tipo in ("Float", "Double"):
            return float(valor)
        return int(valor)

    # ------------------------------------------------------------ NDATA (estado del nodo)
    def _set_nodo(self, nombre: str, valor: Any):
        if self.nodo_valores[nombre] == valor:
            return
        self.nodo_valores[nombre] = valor
        self._nodo_ts[nombre] = self._ms()
        if not self.nacido:
            return                        # el proximo NBIRTH ya lleva el valor actual
        if nombre not in self._pendientes:
            self._pendientes.append(nombre)
        if METRICAS_NODO[nombre][2]:      # Estado: de inmediato
            self._vaciar()

    def _vaciar(self):
        if self._pendientes and self.nacido:
            self._enviar("NDATA", [self._metrica_nodo(n) for n in self._pendientes])
        self._pendientes.clear()
        self._ultimo_flush = self._s()

    def tick(self):
        """Llamar periodicamente (p. ej. cada 250 ms): envia los contadores acumulados."""
        if self._pendientes and self._s() - self._ultimo_flush >= self.periodo_estado_s:
            self._vaciar()

    # ------------------------------------------------------------ comandos (NCMD / DCMD)
    def al_mensaje(self, topico: str, carga: bytes) -> List[Tuple[str, str, Any]]:
        """Procesa un mensaje recibido. Devuelve las acciones para el pegamento OPC UA:
        [("escribir", nombre_metrica, valor), ...]."""
        t = sp.parsear_topico(topico)
        if t is None or t["grupo"] != self.grupo or t["nodo"] != self.nodo:
            return []
        try:
            payload = sp.decodificar(carga)
        except Exception as e:                       # payload invalido: se descarta
            _log.warning("%s: payload invalido (%s)", topico, e)
            return []
        if t["mensaje"] == "NCMD" and t["dispositivo"] is None:
            for m in payload["metricas"]:
                if m["nombre"] == REBIRTH and m["valor"] is True:
                    self.rebirth()
            return []
        if t["mensaje"] == "DCMD" and t["dispositivo"] == self.dispositivo:
            return self._comandos(payload["metricas"])
        return []

    def rebirth(self):
        if not self.nacido:
            _log.info("Rebirth ignorado: no hay sesion nacida")
            return
        _log.info("Rebirth solicitado por NCMD")
        self._set_nodo("Status/Sparkplug/Rebirths", self.nodo_valores["Status/Sparkplug/Rebirths"] + 1)
        self._nbirth()                    # seq = 0 y el mismo bdSeq de la sesion; luego DBIRTH

    def _comandos(self, metricas: List[Dict[str, Any]]) -> List[Tuple[str, str, Any]]:
        acciones = []
        if not self.disp_nacido:
            _log.warning("DCMD ignorado: el dispositivo %s esta fuera de linea", self.dispositivo)
            self.comandos_rechazados += len(metricas)
            return []
        for m in metricas:
            nombre, valor = m["nombre"], m["valor"]
            v = self.variables.get(nombre)
            motivo = None
            if v is None:
                motivo = "metrica desconocida"
            elif not v.escribible:
                motivo = "metrica de solo lectura"
            elif valor is None:
                motivo = "sin valor"
            else:
                try:
                    valor = self._validar(v, valor)
                except ValueError as e:
                    motivo = str(e)
            if motivo:
                _log.warning("DCMD rechazado (%s = %r): %s", nombre, m["valor"], motivo)
                self.comandos_rechazados += 1
                if v is not None:
                    self.reponer(nombre)          # el Host vuelve a ver el valor real
                continue
            acciones.append(("escribir", nombre, valor))
        return acciones

    def _validar(self, v: Variable, valor: Any) -> Any:
        if v.tipo == "Boolean":
            if isinstance(valor, bool) or valor in (0, 1):
                return bool(valor)
            raise ValueError("no es booleano")
        if v.tipo == "String":
            return str(valor)
        if isinstance(valor, bool) or not isinstance(valor, (int, float)):
            raise ValueError("no es numerico")
        valor = int(valor)
        if v.tipo in ("Float", "Double"):
            minimo, maximo = self.limites.get(v.nombre, (float("-inf"), float("inf")))
        else:
            minimo, maximo = self.limites.get(v.nombre, _rango_tipo(v.tipo))
        if not minimo <= valor <= maximo:
            raise ValueError(f"fuera de rango {minimo}..{maximo}")
        return valor

    def reponer(self, nombre: str):
        """Fuerza un DDATA con el valor real conocido (tras un comando rechazado o fallido)."""
        if self.disp_nacido and nombre in self.valores:
            self._enviar("DDATA", [self._metrica_disp(nombre)], True)
            self._publicado[nombre] = self.valores[nombre]

    # ------------------------------------------------------------ cierre ordenado
    def cerrar(self):
        if not self.nacido:
            return
        if self.disp_nacido:
            self._enviar("DDEATH", [], True)
            self.disp_nacido = False
        ahora = self._ms()
        carga = sp.codificar([sp.metrica(BDSEQ, "Int64", self.bd_seq, ahora)], timestamp_ms=ahora)
        self._publicar(self._t("NDEATH"), carga, QOS_NDEATH, False)
        self.estadisticas_msg["NDEATH"] += 1
        self.nacido = False

    def estadisticas(self) -> Dict[str, Any]:
        return {"bdSeq": self.bd_seq, "seq": self.seq, "sesiones": self.sesiones,
                "reconexiones": self.nodo_valores["Status/MQTT/Reconexiones"],
                "rebirths": self.nodo_valores["Status/Sparkplug/Rebirths"],
                "mensajes": dict(self.estadisticas_msg), "dcmd_rechazados": self.comandos_rechazados,
                "dispositivo_en_linea": self.disp_nacido}
