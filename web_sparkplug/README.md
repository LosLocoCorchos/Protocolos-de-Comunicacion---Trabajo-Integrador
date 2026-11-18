# Página web (Host Application Sparkplug B)

| Archivo | Para qué sirve |
|---|---|
| `panel_web.html` | **La página**: un solo archivo con todo adentro (HTML, CSS y JavaScript). Es lo que se abre y se entrega |
| `nucleo.js` | Lógica de la Host Application (Protobuf, estado, seq, Rebirth, STATE, DCMD, diagnóstico). Ya está incluida en `panel_web.html`; se separa para poder probarla en Node |
| `panel_web.template.html`, `construir.py` | Plantilla y script que arman `panel_web.html` (`python3 construir.py`) después de tocar `nucleo.js` |

## Cómo usarla
1. Abrir `panel_web.html` en el navegador (doble clic, o desde una tablet/celular si se publica el archivo, por ejemplo en GitHub Pages o Netlify).
2. Completar la contraseña de `panel_web` (el resto viene precargado: broker `wss://g6a18d21.ala.us-east-1.emqxsl.com:8084/mqtt`, grupo, nodo, dispositivo) y **Conectar**.
3. La página se suscribe a `spBv1.0/UNRaf_PdCI/#`, publica `spBv1.0/STATE/Panel_Web` (retenido, JSON `online`/`timestamp`; el Last Will lo deja en `online:false`) y pide un Rebirth al nodo. Si el gateway ya está corriendo, aparece todo enseguida.

Necesita Internet para bajar `mqtt.js` (unpkg o jsdelivr). Sin acceso, guardar `mqtt.min.js` junto a `panel_web.html` y la página lo toma de ahí.

## Qué muestra
- **Capa por capa** (1 página⇄broker, 2 Edge Node⇄broker, 3 OPC UA, 4 Modbus RS485) con OK / ADVERTENCIA / FALLA / SIN DATOS y un cartel con **dónde** está la falla.
- **Dispositivo 1:** pulsador, potenciómetro, LED digital (interruptor → DCMD) y LED PWM (deslizador 0-255 → DCMD). El valor que se ve es el que confirma el dispositivo (DDATA); mientras espera, dice "enviando comando…".
- **Estadísticas** de Modbus, OPC UA, MQTT y Sparkplug B, **eventos** (NDEATH, DDEATH, saltos de `seq`, recuperaciones) y un **monitor** de mensajes con la trama en hexadecimal y decodificada (sirve para el informe).

## Cómo detecta cada falla
| Falla | Mecanismo | Tiempo aproximado |
|---|---|---|
| Se cae el Dispositivo 3 o su Internet | NDEATH (Last Will); se ignora si su `bdSeq` no es el del último NBIRTH | ~1,5 × keepalive (22 s con keepalive 15 s) |
| Se pierde el esclavo Modbus | `Status/MODBUS/Estado` + DDEATH | ~1,5 s |
| Se cae el enlace OPC UA | `Status/OPCUA/Estado` = ERROR + DDEATH | ~2 s |
| Se pierden mensajes | salto de `seq` → NCMD Rebirth | inmediato |
| Datos sin BIRTH previo | se ignoran y se pide Rebirth | inmediato |
| Esta página pierde Internet | cierre del WebSocket; reconecta sola y pide Rebirth | ~2 s |

## Seguridad
La página no guarda la contraseña. El usuario `panel_web` solo puede suscribirse al grupo y publicar NCMD, DCMD y su STATE (reglas del broker EMQX); cualquiera que abra la página ve ese usuario, por eso tiene permisos mínimos.

Pruebas: `test_web.py` (lógica contra el Edge Node Python real) y `test_web_ui.py` (la página en Chromium con mqtt.js simulado), en `pruebas_t3/`.
