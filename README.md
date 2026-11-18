# Tarea 3 – Integradora: Modbus RTU → OPC UA → MQTT Sparkplug B → página web

**Materia:** Protocolos de Comunicación Industrial · **Carrera:** Ingeniería en Computación · UNRaf
**Autores:** José Ignacio Santagostino, Guillermo Fioramonte
**Profesores:** Milton Pozzo, Martin Picco

Sistema que lleva los datos de un esclavo Modbus RTU (Arduino) hasta una página web pasando por OPC UA y
MQTT con Sparkplug B, con diagnóstico de fallas por capa y recuperación automática.

---

## 1. Arquitectura

```
 Dispositivo 1            Dispositivo 2                  Dispositivo 3                 Broker             Página
 Arduino UNO              Raspberry Pi 4                 Notebook                      EMQX Cloud         (navegador)
 esclavo Modbus  ──RS485──▶ maestro Modbus + ──OPC UA──▶ cliente OPC UA +  ──MQTT/TLS──▶ Serverless ──WSS──▶ Host Application
 (ID 1)          9600 8N1   servidor OPC UA   TCP 4840   Edge Node Sparkplug B  8883                8084      Sparkplug B
```

| Salto | Protocolo | Detalle |
|---|---|---|
| 1 ⇄ 2 | Modbus RTU sobre RS485 | 9600 8N1, CRC-16 (0xA001), silencio t3,5 = 3,65 ms, FC01–FC06 |
| 2 ⇄ 3 | OPC UA (asyncua 1.1.8) | `opc.tcp://192.168.10.1:4840`, modos None / Sign / SignAndEncrypt (Basic256Sha256) |
| 3 ⇄ broker | MQTT 3.1.1 + Sparkplug B | TLS, puerto 8883, payload Protobuf |
| broker ⇄ página | MQTT sobre WebSocket seguro | puerto 8084 (`/mqtt`), mqtt.js v5 |

**Variables** (mismo nombre y tipo en toda la cadena):

| Nombre | Tipo | Área Modbus | Acceso |
|---|---|---|---|
| `Inputs/Pulsador` | Boolean | discrete_input (FC02) | solo lectura |
| `Inputs/Potenciometro` | UInt16 | input_register (FC04) | solo lectura |
| `Outputs/LED_Digital` | Boolean | coil (FC01/FC05) | lectura y escritura |
| `Outputs/LED_PWM` | UInt16 (0–255) | holding_register (FC03/FC06) | lectura y escritura |

**Sparkplug B:** grupo `UNRaf_PdCI`, nodo `Gateway_OPCUA`, dispositivo `Esclavo_Modbus_ID1`, host `Panel_Web`.
NodeIds OPC UA: `ns=1;s=Esclavo_Modbus_ID1.Inputs|Outputs.<nombre>` y `Status.MODBUS.{Estado,OK,CRC_ERROR,TIMEOUT,Alcanzable}`.

---

## 2. Estructura del repositorio

| Carpeta / archivo | Contenido |
|---|---|
| `Arduino (Esclavo)/esclavo_modbus_t3/` | `esclavo_modbus_t3.ino`: esclavo Modbus programado a mano (CRC, silencio, excepciones) |
| `Raspberry/` | `maestro_modbus.py` (maestro), `servidor_opcua_t3.py` (servidor OPC UA), `puente_opcua.py` (une Modbus con OPC UA), `config_esclavo.json` |
| `notebook_sparkplug/` | `gateway_sparkplug.py` (cliente OPC UA + Edge Node), `edge_node.py` (lógica Sparkplug B), `sparkplug_b.py` / `sparkplug_b.proto`, `generar_certificados.py`, `config_gateway.json`, `config_esclavo.json` (copia del de la Raspberry), `requirements.txt`, `LEEME.md` |
| `web_sparkplug/` | `panel_web.html` (la página, un solo archivo), `nucleo.js` (lógica), `panel_web.template.html` y `construir.py`, `LEEME.md` |
| `panel_web.html` (raíz) | Copia de la página, idéntica a `web_sparkplug/panel_web.html` |

> Las pruebas automáticas, el informe técnico y las reglas del broker no están en este repositorio. Las reglas de permisos del broker se resumen en la sección 6.

---

## 3. Lo realizado

- **Esclavo Modbus (Arduino):** FC01–FC06, CRC-16 y silencio t3,5 propios, excepciones 01/02/03, broadcast solo para escrituras, inyección de falla de CRC con el comando `c` por el monitor serie.
- **Maestro Modbus (Raspberry):** una única función de transacción, estados DONE / CRC ERROR / DEVICE NOT REACHED (3 ciclos sin respuesta), contadores, poller cada 500 ms con timeout de 300 ms, modo `--debug` con tramas TX/RX en hexadecimal.
- **Servidor OPC UA:** espacio de direcciones con las variables y el estado del maestro, tres modos de seguridad, usuario `operador` o anónimo, suscripciones; el puente evita el lazo de eco y relee el esclavo tras cada escritura.
- **Gateway / Edge Node Sparkplug B:** NBIRTH, DBIRTH, NDATA, DDATA, DDEATH, NDEATH (Last Will con `bdSeq`), NCMD/DCMD, Rebirth, STATE; validación de comandos (PWM 0–255).
- **Broker:** EMQX Cloud Serverless con TLS, usuarios `gateway_opcua` y `panel_web` y lista blanca de tópicos.
- **Página web:** estado capa por capa (página⇄broker, Edge Node⇄broker, OPC UA, Modbus), tarjetas de las variables con comandos (DCMD), estadísticas, eventos y monitor de mensajes.
- **Pruebas:** automáticas sin hardware (no incluidas en este repositorio) y funcionales con el equipo real (arranque, lecturas, comandos, Rebirth, CRC, esclavo ausente, caída del Edge Node, servidor OPC UA caído, seguridad, cliente no confiable, clave incorrecta, MQTT Explorer).
- **Informe técnico:** PDF con arquitectura, modelo de información, análisis de tramas, diseño lógico, Sparkplug, seguridad, pruebas, librerías y anexos (se entrega aparte).

---

## 4. Puesta en marcha

Orden: **esclavo → Raspberry → notebook → página.**

### Requisitos
- **Raspberry:** Python 3.12 (asyncua 1.1.8 no corre en 3.14; usar un entorno virtual), `pip install asyncua pyserial cryptography`.
- **Notebook:** `pip install -r notebook_sparkplug/requirements.txt` (asyncua, paho-mqtt ≥ 2, protobuf, cryptography).
- Red cableada Raspberry ⇄ notebook (`192.168.10.1` la Raspberry) y acceso a Internet para el broker.

### 1) Esclavo
Abrir `Arduino (Esclavo)/esclavo_modbus_t3/esclavo_modbus_t3.ino` en el IDE de Arduino y cargarlo (`SLAVE_ID = 1`). Cableado: Arduino ⇄ MAX485 (SoftwareSerial pines 10/11, DE+RE pin 8) ⇄ adaptador USB-RS485 de la Raspberry (A con A, B con B, GND común).

### 2) Raspberry (maestro + servidor OPC UA)
```bash
cd Raspberry
source venv/bin/activate
python servidor_opcua_t3.py --puerto /dev/ttyUSB0 --baudrate 9600 --id 1      # --debug muestra las tramas
python maestro_modbus.py --debug                                              # maestro solo, para probar el enlace
```

### 3) Notebook (gateway)
```bash
cd notebook_sparkplug
pip install -r requirements.txt
export MQTT_CLAVE='[clave del usuario gateway_opcua]'      # no escribir la clave en config_gateway.json
python gateway_sparkplug.py --broker g6a18d21.ala.us-east-1.emqxsl.com \
  --mqtt-usuario gateway_opcua --url opc.tcp://192.168.10.1:4840 --seguridad none
```
Otros modos: `--seguridad sign` o `--seguridad encrypt` (con certificados), y `--usuario operador --clave [clave]`.
Cerrar con **Ctrl+C** (publica DDEATH y NDEATH).

### 4) Página
Abrir `web_sparkplug/panel_web.html` (o la copia de la raíz), completar la contraseña del usuario `panel_web` (no se guarda) y **Conectar**.

### Certificados (Sign / SignAndEncrypt)
```bash
# el script está en notebook_sparkplug/; copiarlo también a la Raspberry
python generar_certificados.py servidor --ip 192.168.10.1     # en la Raspberry
python generar_certificados.py cliente  --ip [IP notebook]    # en la notebook
```
Los archivos `.pem` (claves privadas) **no se suben nunca al repositorio** y nunca se intercambian: solo se copian los `.der`. Copiar el `.der` del cliente a `pki/confianza/` de la Raspberry y **reiniciar el servidor** (lee la lista de confianza solo al arrancar) y el `.der` del servidor a la notebook.

---

## 5. Configuración del esclavo (`config_esclavo.json`)

El mismo archivo debe estar en `Raspberry/` y en `notebook_sparkplug/`. Campos: `serie` (puerto, baudrate, paridad, bits de parada, timeout), `esclavo_id`, `polling_ms`, `ciclos_para_perdido` y `variables` (`nombre`, `tipo`, `area`, `direccion`).
Áreas: `discrete_input`, `input_register`, `coil`, `holding_register`. Tipos: `Boolean` (áreas de bits), `UInt16`/`Int16` (registros).
Por línea de comandos se pueden sobrescribir `--puerto --baudrate --paridad --bits-parada --id --timeout-ms --polling-ms`.

**Cambiar a otro esclavo (p. ej. ID 2):**
1. Cambiar `SLAVE_ID` en `Arduino (Esclavo)/esclavo_modbus_t3/esclavo_modbus_t3.ino` y volver a cargarlo.
2. Cambiar `"esclavo_id"` (y las variables si difieren) en `config_esclavo.json` de la Raspberry **y** de la notebook; reiniciar servidor y gateway.
3. En el broker, ampliar la regla DCMD del usuario `gateway_opcua` a `.../DCMD/Gateway_OPCUA/+` (hoy solo permite `Esclavo_Modbus_ID1`; ver sección 6).
4. Cambiar `Esclavo_Modbus_ID1` en la página (campo "Dispositivo" o `panel_web.html`).

> Limitación actual: el sistema atiende **un esclavo por ejecución**. Dos esclavos simultáneos requieren cambios de código (lista de esclavos en la configuración, un poller y un dispositivo Sparkplug por esclavo, tarjetas por dispositivo en la página).

---

## 6. Broker MQTT (EMQX) y pruebas

**Usuarios y permisos por tópico** (lista blanca; todo lo demás se deniega):

| Usuario | Acción | Tópico |
|---|---|---|
| `gateway_opcua` | publicar | `spBv1.0/UNRaf_PdCI/+/Gateway_OPCUA` y `.../+/Gateway_OPCUA/+` |
| `gateway_opcua` | suscribirse | `spBv1.0/UNRaf_PdCI/NCMD/Gateway_OPCUA` y `.../DCMD/Gateway_OPCUA/Esclavo_Modbus_ID1` |
| `panel_web` | suscribirse | `spBv1.0/UNRaf_PdCI/#` |
| `panel_web` | publicar | `.../NCMD/Gateway_OPCUA`, `.../DCMD/Gateway_OPCUA/+`, `spBv1.0/STATE/Panel_Web` |

**Cómo ver el CRC:** los dos últimos bytes de cada trama (byte bajo primero). Ejemplo: `01 03 00 00 00 01 84 0A`. Con `--debug` se ve en las líneas TX/RX; el contador `CRC_ERROR` está en el monitor, en el nodo `Status.MODBUS.CRC_ERROR` y en la página. Para provocar el error en vivo: enviar `c` por el monitor serie del Arduino.

**Verificar el tráfico:** con MQTT Explorer conectado al broker se ve el árbol `spBv1.0/UNRaf_PdCI/...` con los mensajes NBIRTH, DBIRTH, NDATA, DDATA, DDEATH, NDEATH, NCMD y DCMD.

---

## 7. Problemas frecuentes

| Síntoma | Causa probable |
|---|---|
| `asyncua` no importa | Python 3.14: usar un entorno virtual con Python 3.12 |
| `BadCertificateUntrusted` | El certificado del cliente no está en `pki/confianza/` o no se reinició el servidor |
| `BadUserAccessDenied` | Usuario o contraseña OPC UA incorrectos |
| `Bad username or password` en la página | Clave MQTT incorrecta del usuario `panel_web` |
| La página no comanda otro esclavo | Regla DCMD del broker limitada a `Esclavo_Modbus_ID1` |
| `DEVICE NOT REACHED` | Esclavo apagado, cable A/B invertido, ID o velocidad distintos |
| Sin NBIRTH en la página | Gateway no conectado al broker; tocar **Solicitar Rebirth** |
