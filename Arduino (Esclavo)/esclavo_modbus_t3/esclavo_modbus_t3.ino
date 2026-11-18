/*
  ESCLAVO MODBUS RTU con parser y CRC-16 PROPIOS
  TP3 Integradora - Protocolos de Comunicaciones Industriales - UNRaf
  Placa: Arduino Uno + MAX485

  Diferencia con la Tarea 1: ya NO se usa la libreria ModbusRTUSlave.
  La trama se arma, se valida y se responde "a mano" (consigna 1.a):
    - deteccion de fin de trama por silencio (t3,5)
    - verificacion del CRC-16 de cada solicitud
    - generacion del CRC-16 de cada respuesta
    - respuestas de excepcion Modbus

  Mapa de registros (direccion en la trama, base 0):
    Discrete Input 0 (10001) -> Pulsador              FC02
    Input Register 0 (30001) -> Potenciometro (ADC)   FC04
    Coil           0 (00001) -> LED digital           FC01 lee / FC05 escribe
    Holding Reg.   0 (40001) -> LED PWM (0-255)       FC03 lee / FC06 escribe

  Conexion MAX485:
    RO -> pin 10 (SoftwareSerial RX)    DI -> pin 11 (SoftwareSerial TX)
    DE y RE unidos -> pin 8             A/B -> bus RS485

  Puerto USB (Serial) a 115200: solo logs de depuracion.
  Comando por el monitor serie:  c  -> corrompe el CRC de la PROXIMA respuesta
                                       (para provocar un "CRC ERROR" en el maestro).
*/

#include <SoftwareSerial.h>

// ====================== Pines ======================
const byte PIN_RX      = 10;   // RO del MAX485
const byte PIN_TX      = 11;   // DI del MAX485
const byte PIN_DE_RE   = 8;    // DE y RE unidos
const byte PIN_SWITCH  = 2;    // pulsador (pull-up interno, activo en bajo)
const byte PIN_POT     = A0;   // potenciometro
const byte PIN_LED1    = 6;    // LED digital  (Coil 0)
const byte PIN_LED2PWM = 9;    // LED PWM      (Holding Register 0)

// ====================== Parametros del enlace ======================
const byte          SLAVE_ID   = 1;        // direccion de este esclavo (1..247)
const unsigned long BAUD_BUS   = 9600;     // bus Modbus, formato 8N1 (10 bits por caracter)
const unsigned long BAUD_DEBUG = 115200;   // monitor serie (USB)

// Silencio que marca el fin de una trama RTU: 3,5 caracteres.
// 3,5 caracteres x 10 bits = 35 bits -> t3,5 = 35 / BAUD  (en microsegundos).
// A 9600 bps: 35 / 9600 = 3,65 ms.
const unsigned long T35_US = 35000000UL / BAUD_BUS;

const byte PWM_MAX = 255;                  // el Holding Register 0 acepta 0..255

// ====================== Codigos Modbus ======================
const byte FC_READ_COILS       = 0x01;
const byte FC_READ_DISCRETE    = 0x02;
const byte FC_READ_HOLDING     = 0x03;
const byte FC_READ_INPUT       = 0x04;
const byte FC_WRITE_COIL       = 0x05;
const byte FC_WRITE_HOLDING    = 0x06;

const byte EX_ILLEGAL_FUNCTION = 0x01;     // funcion no soportada
const byte EX_ILLEGAL_ADDRESS  = 0x02;     // direccion/rango fuera de la tabla
const byte EX_ILLEGAL_VALUE    = 0x03;     // cantidad o valor invalido

// ====================== Tablas de datos ======================
const byte N_COILS    = 1;
const byte N_DISCRETE = 1;
const byte N_HOLDING  = 1;
const byte N_INPUT    = 1;

bool     coils[N_COILS];
bool     discreteInputs[N_DISCRETE];
uint16_t holdingRegs[N_HOLDING];
uint16_t inputRegs[N_INPUT];

// ====================== Buffers y estado ======================
const byte MAX_FRAME = 64;                 // soporta tablas de hasta ~29 registros
byte rxBuf[MAX_FRAME];
byte rxLen = 0;
bool rxOverflow = false;
unsigned long lastByteUs = 0;              // instante del ultimo byte leido
byte txBuf[MAX_FRAME];

bool inyectarFalloCRC = false;             // se activa con el comando 'c'

// Contadores locales (solo para depuracion en este esclavo)
unsigned int statOk = 0;                   // solicitudes validas respondidas
unsigned int statExc = 0;                  // respondidas con excepcion
unsigned int statCrc = 0;                  // solicitudes descartadas por CRC invalido
unsigned int statMal = 0;                  // tramas demasiado cortas / desbordadas / mal formadas
unsigned int statAjena = 0;                // tramas dirigidas a otro esclavo

SoftwareSerial modbusSerial(PIN_RX, PIN_TX);

unsigned long lastLogMs = 0;
bool lastCoil = false;
uint16_t lastHolding = 0;

// =====================================================================
//  CRC-16 MODBUS
//  Polinomio 0xA001 (0x8005 reflejado), valor inicial 0xFFFF.
//  Se procesa bit a bit: por cada byte se hace XOR con el CRC y luego
//  8 desplazamientos a la derecha; si el bit que sale es 1, se aplica
//  XOR con 0xA001.
//  En la trama el CRC viaja con el byte BAJO primero.
// =====================================================================
uint16_t crc16(const byte *datos, byte largo) {
  uint16_t crc = 0xFFFF;
  for (byte i = 0; i < largo; i++) {
    crc ^= datos[i];
    for (byte bit = 0; bit < 8; bit++) {
      if (crc & 0x0001) {
        crc = (crc >> 1) ^ 0xA001;
      } else {
        crc >>= 1;
      }
    }
  }
  return crc;
}

// Arma una respuesta de excepcion: [ID][FC|0x80][codigo]. Devuelve el largo (sin CRC).
byte armarExcepcion(const byte *req, byte *resp, byte codigo) {
  resp[0] = req[0];
  resp[1] = req[1] | 0x80;
  resp[2] = codigo;
  return 3;
}

// =====================================================================
//  Procesa una solicitud ya validada (CRC correcto y direccion propia).
//  req   : trama completa recibida (incluye los 2 bytes de CRC)
//  largo : largo total de req
//  resp  : buffer donde se arma la respuesta SIN CRC
//  Devuelve el largo de la respuesta (sin CRC), o 0 si no hay que responder.
//
//  Orden de validacion (especificacion Modbus):
//    1) funcion soportada  -> si no, excepcion 01
//    2) cantidad / valor   -> si no, excepcion 03
//    3) direccion y rango  -> si no, excepcion 02
// =====================================================================
byte procesarSolicitud(const byte *req, byte largo, byte *resp) {
  byte fc = req[1];

  if (fc < FC_READ_COILS || fc > FC_WRITE_HOLDING) {
    return armarExcepcion(req, resp, EX_ILLEGAL_FUNCTION);
  }
  // Las 6 funciones soportadas tienen solicitud de exactamente 8 bytes:
  // ID(1) + FC(1) + direccion(2) + cantidad o valor(2) + CRC(2)
  if (largo != 8) {
    return 0;
  }

  uint16_t dir = ((uint16_t)req[2] << 8) | req[3];   // big-endian
  uint16_t arg = ((uint16_t)req[4] << 8) | req[5];   // cantidad (FC01-04) o valor (FC05-06)

  resp[0] = req[0];
  resp[1] = fc;

  switch (fc) {

    case FC_READ_COILS:
    case FC_READ_DISCRETE: {
      const bool *tabla = (fc == FC_READ_COILS) ? coils : discreteInputs;
      uint16_t n = (fc == FC_READ_COILS) ? N_COILS : N_DISCRETE;
      if (arg < 1 || arg > 2000) return armarExcepcion(req, resp, EX_ILLEGAL_VALUE);
      if (dir >= n || (uint32_t)dir + arg > n) return armarExcepcion(req, resp, EX_ILLEGAL_ADDRESS);
      byte nBytes = (arg + 7) / 8;                   // bits empaquetados, LSB primero
      resp[2] = nBytes;
      for (byte i = 0; i < nBytes; i++) resp[3 + i] = 0;
      for (uint16_t i = 0; i < arg; i++) {
        if (tabla[dir + i]) resp[3 + i / 8] |= (1 << (i % 8));
      }
      return 3 + nBytes;
    }

    case FC_READ_HOLDING:
    case FC_READ_INPUT: {
      const uint16_t *tabla = (fc == FC_READ_HOLDING) ? holdingRegs : inputRegs;
      uint16_t n = (fc == FC_READ_HOLDING) ? N_HOLDING : N_INPUT;
      if (arg < 1 || arg > 125) return armarExcepcion(req, resp, EX_ILLEGAL_VALUE);
      if (dir >= n || (uint32_t)dir + arg > n) return armarExcepcion(req, resp, EX_ILLEGAL_ADDRESS);
      resp[2] = arg * 2;                             // cantidad de bytes de datos
      for (uint16_t i = 0; i < arg; i++) {
        resp[3 + 2 * i] = tabla[dir + i] >> 8;       // byte alto primero
        resp[4 + 2 * i] = tabla[dir + i] & 0xFF;
      }
      return 3 + arg * 2;
    }

    case FC_WRITE_COIL: {
      // valor valido: 0xFF00 = ON, 0x0000 = OFF
      if (arg != 0xFF00 && arg != 0x0000) return armarExcepcion(req, resp, EX_ILLEGAL_VALUE);
      if (dir >= N_COILS) return armarExcepcion(req, resp, EX_ILLEGAL_ADDRESS);
      coils[dir] = (arg == 0xFF00);
      memcpy(resp + 2, req + 2, 4);                  // la respuesta repite direccion y valor
      return 6;
    }

    case FC_WRITE_HOLDING: {
      // El LED PWM solo admite 0..255: un valor mayor es un dato invalido
      if (arg > PWM_MAX) return armarExcepcion(req, resp, EX_ILLEGAL_VALUE);
      if (dir >= N_HOLDING) return armarExcepcion(req, resp, EX_ILLEGAL_ADDRESS);
      holdingRegs[dir] = arg;
      memcpy(resp + 2, req + 2, 4);                  // la respuesta repite direccion y valor
      return 6;
    }
  }
  return 0;
}

// Agrega el CRC a la respuesta y la transmite por el MAX485 (half-duplex).
void enviarRespuesta(byte *resp, byte largo) {
  uint16_t crc = crc16(resp, largo);
  resp[largo]     = crc & 0xFF;          // CRC byte bajo primero
  resp[largo + 1] = crc >> 8;

  if (inyectarFalloCRC) {                // prueba de falla: se invierte un bit del CRC
    resp[largo + 1] ^= 0x01;
    inyectarFalloCRC = false;
    Serial.println(F("[FALLA] CRC corrupto en esta respuesta"));
  }

  digitalWrite(PIN_DE_RE, HIGH);         // DE=1 y RE=1: transmitir (receptor deshabilitado)
  modbusSerial.write(resp, largo + 2);
  modbusSerial.flush();
  delayMicroseconds(100);                // margen para que salga el ultimo bit
  digitalWrite(PIN_DE_RE, LOW);          // volver a recepcion
  while (modbusSerial.available()) modbusSerial.read();   // descarta ruido de la conmutacion
}

// Se llama cuando pasaron >= t3,5 sin bytes nuevos: hay una trama completa en rxBuf.
void tramaCompleta() {
  byte largo = rxLen;
  bool desbordo = rxOverflow;
  rxLen = 0;
  rxOverflow = false;

  // Minimo: ID + FC + CRC(2) = 4 bytes
  if (desbordo || largo < 4) {
    statMal++;
    return;
  }

  // Verificacion del CRC: se recalcula sobre todo menos los 2 ultimos bytes
  uint16_t crcRecibido = rxBuf[largo - 2] | ((uint16_t)rxBuf[largo - 1] << 8);
  uint16_t crcCalculado = crc16(rxBuf, largo - 2);
  if (crcRecibido != crcCalculado) {
    statCrc++;
    Serial.print(F("[CRC] descartada, rx=0x"));
    Serial.print(crcRecibido, HEX);
    Serial.print(F(" calc=0x"));
    Serial.println(crcCalculado, HEX);
    return;                              // una trama con CRC malo se descarta sin responder
  }

  byte id = rxBuf[0];
  if (id == 0) {                         // broadcast: solo escrituras, nunca se responde
    byte fc = rxBuf[1];
    if (fc == FC_WRITE_COIL || fc == FC_WRITE_HOLDING) procesarSolicitud(rxBuf, largo, txBuf);
    return;
  }
  if (id != SLAVE_ID) {                  // dirigida a otro esclavo: se ignora
    statAjena++;
    return;
  }

  byte largoResp = procesarSolicitud(rxBuf, largo, txBuf);
  if (largoResp == 0) {                  // solicitud mal formada: sin respuesta
    statMal++;
    return;
  }
  if (txBuf[1] & 0x80) statExc++; else statOk++;
  enviarRespuesta(txBuf, largoResp);
}

// Lee los bytes disponibles del bus y los acumula en el buffer de trama.
void recibirBytes() {
  while (modbusSerial.available()) {
    byte b = modbusSerial.read();
    lastByteUs = micros();
    if (rxLen < MAX_FRAME) rxBuf[rxLen++] = b;
    else rxOverflow = true;
  }
}

void setup() {
  Serial.begin(BAUD_DEBUG);
  Serial.println(F("Esclavo Modbus RTU (CRC propio) iniciando"));
  Serial.print(F("ID="));
  Serial.print(SLAVE_ID);
  Serial.print(F(" baud="));
  Serial.print(BAUD_BUS);
  Serial.print(F(" t3.5(us)="));
  Serial.println(T35_US);

  pinMode(PIN_SWITCH, INPUT_PULLUP);
  pinMode(PIN_LED1, OUTPUT);
  pinMode(PIN_LED2PWM, OUTPUT);
  pinMode(PIN_DE_RE, OUTPUT);
  digitalWrite(PIN_DE_RE, LOW);          // arranca en recepcion
  modbusSerial.begin(BAUD_BUS);
  Serial.println(F("Comando 'c': corrompe el CRC de la proxima respuesta"));
}

void loop() {
  // 1) Entradas
  discreteInputs[0] = !digitalRead(PIN_SWITCH);
  inputRegs[0] = analogRead(PIN_POT);

  // 2) Bus Modbus: acumular bytes y, tras el silencio t3,5, procesar la trama
  recibirBytes();
  if (rxLen > 0 && (micros() - lastByteUs) >= T35_US) {
    tramaCompleta();
  }

  // 3) Salidas
  digitalWrite(PIN_LED1, coils[0]);
  analogWrite(PIN_LED2PWM, holdingRegs[0]);

  // 4) Depuracion por USB
  while (Serial.available()) {
    if (Serial.read() == 'c') {
      inyectarFalloCRC = true;
      Serial.println(F("[FALLA] armada: proxima respuesta con CRC malo"));
    }
  }
  if (coils[0] != lastCoil) {
    Serial.print(F("[EVT] LED1="));
    Serial.println(coils[0]);
    lastCoil = coils[0];
  }
  if (holdingRegs[0] != lastHolding) {
    Serial.print(F("[EVT] PWM="));
    Serial.println(holdingRegs[0]);
    lastHolding = holdingRegs[0];
  }
  unsigned long ahora = millis();
  if (ahora - lastLogMs >= 1000) {
    lastLogMs = ahora;
    Serial.print(F("SW="));
    Serial.print(discreteInputs[0]);
    Serial.print(F(" POT="));
    Serial.print(inputRegs[0]);
    Serial.print(F(" ok="));
    Serial.print(statOk);
    Serial.print(F(" crc="));
    Serial.print(statCrc);
    Serial.print(F(" exc="));
    Serial.println(statExc);
  }
}
