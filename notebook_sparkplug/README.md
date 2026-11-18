# Dispositivo 3 (notebook): cliente OPC UA + Edge Node Sparkplug B

| Archivo | Para qué sirve |
|---|---|
| `gateway_sparkplug.py` | Programa principal: sesión OPC UA (cliente del TP2) + sesión MQTT + consola de estadísticas |
| `edge_node.py` | Lógica del Edge Node: NBIRTH/DBIRTH/NDATA/DDATA/DDEATH/NDEATH, seq, bdSeq, Rebirth, DCMD (no depende de MQTT ni OPC UA) |
| `sparkplug_b.py` | Codificación/decodificación del payload Protobuf (sin protoc) |
| `sparkplug_b.proto` | Subconjunto del `sparkplug_b.proto` oficial (mismos números de campo); lo puede usar la página web |
| `config_gateway.json` | Broker, seguridad OPC UA, group_id / edge_node_id, límites de comandos |
| `config_esclavo.json` | Copia del archivo de la Raspberry (define las variables del dispositivo y su ID) |

## Instalar y correr

    pip install -r requirements.txt
    python gateway_sparkplug.py --config config_gateway.json

Hay que completar en `config_gateway.json` (o por línea de comandos) el broker, el usuario y la clave
(`--broker`, `--mqtt-usuario`, `--mqtt-clave`, o la variable de entorno `MQTT_CLAVE`).
La seguridad OPC UA se elige igual que en el TP2: `--seguridad none|sign|encrypt`, `--usuario`, `--clave`.
Si el esclavo de la cátedra tiene otro ID, se cambia en `config_esclavo.json` de **ambos** dispositivos
(el `device_id` y los NodeIds se arman con ese número).

## Qué se ve en la consola (una línea por segundo, todas las capas)

    MODBUS DONE OK=.. CRC=.. TO=.. | OPCUA DONE esc.ok=.. esc.err=.. reconex=.. | MQTT CONECTADO reconex=.. rx=.. |
    SPB bdSeq=.. seq=.. rebirths=.. NBIRTH=.. DBIRTH=.. NDATA=.. DDATA=.. DDEATH=.. dispositivo=EN LINEA

## Mensajes y QoS

| Mensaje | Tópico | QoS | Retain |
|---|---|---|---|
| NBIRTH, NDATA | `spBv1.0/UNRaf_PdCI/{NBIRTH,NDATA}/Gateway_OPCUA` | 0 | no |
| DBIRTH, DDATA, DDEATH | `spBv1.0/UNRaf_PdCI/{...}/Gateway_OPCUA/Esclavo_Modbus_ID1` | 0 | no |
| NDEATH (Will y cierre ordenado) | `spBv1.0/UNRaf_PdCI/NDEATH/Gateway_OPCUA` | 1 | no |
| NCMD, DCMD (se reciben) | `.../NCMD/Gateway_OPCUA`, `.../DCMD/Gateway_OPCUA/Esclavo_Modbus_ID1` | suscripción QoS 1 | - |

Pruebas sin hardware (en `pruebas_t3/`): `test_edge.py` (lógica Sparkplug) y `test_gateway_stub.py`
(gateway con asyncua y paho simulados).
