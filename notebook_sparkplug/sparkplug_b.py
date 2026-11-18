"""
Codificacion Sparkplug B (payload Protobuf) - TP3 Integradora | PdCI | UNRaf

Usa solo la libreria `protobuf` (pip install protobuf). El esquema (subconjunto de sparkplug_b.proto:
Payload y Metric, con los numeros de campo oficiales) se arma en memoria con descriptor_pb2, asi no
hace falta protoc ni un archivo *_pb2.py generado. sparkplug_b.proto documenta el mismo esquema.

  codificar(metricas, seq=None, timestamp_ms=None) -> bytes
  decodificar(bytes) -> {"timestamp": int|None, "seq": int|None, "metricas": [Metrica, ...]}

Una metrica es un dict: {"nombre", "tipo", "valor", "timestamp"}; valor None se codifica con is_null.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

# ---------------------------------------------------------------- tipos de dato Sparkplug B
TIPOS = {"Int8": 1, "Int16": 2, "Int32": 3, "Int64": 4, "UInt8": 5, "UInt16": 6, "UInt32": 7,
         "UInt64": 8, "Float": 9, "Double": 10, "Boolean": 11, "String": 12}
NOMBRE_TIPO = {v: k for k, v in TIPOS.items()}
# Campo de Metric.value en el que viaja cada tipo
CAMPO_VALOR = {"Int8": "int_value", "Int16": "int_value", "Int32": "int_value", "UInt8": "int_value",
               "UInt16": "int_value", "UInt32": "int_value", "Int64": "long_value",
               "UInt64": "long_value", "Float": "float_value", "Double": "double_value",
               "Boolean": "boolean_value", "String": "string_value"}
_CON_SIGNO = {"Int8": 8, "Int16": 16, "Int32": 32, "Int64": 64}

# ---------------------------------------------------------------- namespace y tipos de mensaje
NAMESPACE = "spBv1.0"
MENSAJES = ("NBIRTH", "NDEATH", "NDATA", "NCMD", "DBIRTH", "DDEATH", "DDATA", "DCMD")


def topico(grupo: str, mensaje: str, nodo: str, dispositivo: Optional[str] = None) -> str:
    """spBv1.0/{group_id}/{message_type}/{edge_node_id}[/{device_id}]"""
    t = f"{NAMESPACE}/{grupo}/{mensaje}/{nodo}"
    return t if dispositivo is None else f"{t}/{dispositivo}"


def parsear_topico(t: str) -> Optional[Dict[str, Optional[str]]]:
    """Devuelve grupo/mensaje/nodo/dispositivo, o None si no es un topico Sparkplug de nodo/dispositivo."""
    p = t.split("/")
    if len(p) not in (4, 5) or p[0] != NAMESPACE or p[2] not in MENSAJES:
        return None
    return {"grupo": p[1], "mensaje": p[2], "nodo": p[3], "dispositivo": p[4] if len(p) == 5 else None}


# ---------------------------------------------------------------- esquema protobuf en memoria
def _construir_clases():
    F = descriptor_pb2.FieldDescriptorProto
    fd = descriptor_pb2.FileDescriptorProto()
    fd.name = "sparkplug_b_subset.proto"
    fd.package = "org.eclipse.tahu.protobuf"
    fd.syntax = "proto2"
    payload = fd.message_type.add()
    payload.name = "Payload"
    metric = payload.nested_type.add()
    metric.name = "Metric"

    def campo(msg, nombre, numero, tipo, rotulo=F.LABEL_OPTIONAL, tipo_nombre=None, oneof=None):
        c = msg.field.add()
        c.name, c.number, c.type, c.label = nombre, numero, tipo, rotulo
        if tipo_nombre:
            c.type_name = tipo_nombre
        if oneof is not None:
            c.oneof_index = oneof
        return c

    campo(metric, "name", 1, F.TYPE_STRING)
    campo(metric, "alias", 2, F.TYPE_UINT64)
    campo(metric, "timestamp", 3, F.TYPE_UINT64)
    campo(metric, "datatype", 4, F.TYPE_UINT32)
    campo(metric, "is_historical", 5, F.TYPE_BOOL)
    campo(metric, "is_transient", 6, F.TYPE_BOOL)
    campo(metric, "is_null", 7, F.TYPE_BOOL)
    metric.oneof_decl.add().name = "value"
    campo(metric, "int_value", 10, F.TYPE_UINT32, oneof=0)
    campo(metric, "long_value", 11, F.TYPE_UINT64, oneof=0)
    campo(metric, "float_value", 12, F.TYPE_FLOAT, oneof=0)
    campo(metric, "double_value", 13, F.TYPE_DOUBLE, oneof=0)
    campo(metric, "boolean_value", 14, F.TYPE_BOOL, oneof=0)
    campo(metric, "string_value", 15, F.TYPE_STRING, oneof=0)
    campo(metric, "bytes_value", 16, F.TYPE_BYTES, oneof=0)

    campo(payload, "timestamp", 1, F.TYPE_UINT64)
    campo(payload, "metrics", 2, F.TYPE_MESSAGE, F.LABEL_REPEATED,
          ".org.eclipse.tahu.protobuf.Payload.Metric")
    campo(payload, "seq", 3, F.TYPE_UINT64)
    campo(payload, "uuid", 4, F.TYPE_STRING)
    campo(payload, "body", 5, F.TYPE_BYTES)

    pool = descriptor_pool.DescriptorPool()
    pool.Add(fd)
    desc = pool.FindMessageTypeByName("org.eclipse.tahu.protobuf.Payload")
    try:
        return message_factory.GetMessageClass(desc)
    except AttributeError:                                   # protobuf < 4.22
        return message_factory.MessageFactory(pool).GetPrototype(desc)


Payload = _construir_clases()


# ---------------------------------------------------------------- codificacion / decodificacion
def _a_entero_sin_signo(tipo: str, valor: int) -> int:
    """Int8/16/32 viajan en uint32 y Int64 en uint64, en complemento a dos (como en Tahu)."""
    if tipo == "Int64":
        return int(valor) & ((1 << 64) - 1)
    if tipo in _CON_SIGNO:
        return int(valor) & ((1 << 32) - 1)
    return int(valor)


def _desde_entero_sin_signo(tipo: str, crudo: int) -> int:
    if tipo not in _CON_SIGNO:
        return crudo
    bits = _CON_SIGNO[tipo]
    crudo &= (1 << bits) - 1
    return crudo - (1 << bits) if crudo >= 1 << (bits - 1) else crudo


def metrica(nombre: str, tipo: str, valor: Any, timestamp_ms: Optional[int] = None) -> Dict[str, Any]:
    return {"nombre": nombre, "tipo": tipo, "valor": valor, "timestamp": timestamp_ms}


def codificar(metricas: List[Dict[str, Any]], seq: Optional[int] = None,
              timestamp_ms: Optional[int] = None) -> bytes:
    p = Payload()
    if timestamp_ms is not None:
        p.timestamp = int(timestamp_ms)
    if seq is not None:
        if not 0 <= seq <= 255:
            raise ValueError(f"seq fuera de 0..255: {seq}")
        p.seq = seq
    for m in metricas:
        tipo = m["tipo"]
        if tipo not in TIPOS:
            raise ValueError(f"tipo de dato Sparkplug desconocido: {tipo}")
        x = p.metrics.add()
        x.name = m["nombre"]
        if m.get("timestamp") is not None:
            x.timestamp = int(m["timestamp"])
        x.datatype = TIPOS[tipo]
        valor = m.get("valor")
        if valor is None:
            x.is_null = True
            continue
        campo = CAMPO_VALOR[tipo]
        if campo in ("int_value", "long_value"):
            setattr(x, campo, _a_entero_sin_signo(tipo, valor))
        elif campo == "boolean_value":
            x.boolean_value = bool(valor)
        elif campo == "string_value":
            x.string_value = str(valor)
        else:
            setattr(x, campo, float(valor))
    return p.SerializeToString()


def decodificar(datos: bytes) -> Dict[str, Any]:
    p = Payload()
    p.ParseFromString(datos)                  # lanza google.protobuf.message.DecodeError si es invalido
    metricas = []
    for x in p.metrics:
        tipo = NOMBRE_TIPO.get(x.datatype, f"Desconocido({x.datatype})")
        if x.HasField("is_null") and x.is_null:
            valor = None
        else:
            donde = x.WhichOneof("value")
            valor = None if donde is None else getattr(x, donde)
            if donde in ("int_value", "long_value"):
                valor = _desde_entero_sin_signo(tipo, valor)
        metricas.append({"nombre": x.name if x.HasField("name") else None, "tipo": tipo, "valor": valor,
                         "timestamp": x.timestamp if x.HasField("timestamp") else None})
    return {"timestamp": p.timestamp if p.HasField("timestamp") else None,
            "seq": p.seq if p.HasField("seq") else None, "metricas": metricas}
