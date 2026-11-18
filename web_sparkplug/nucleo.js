"use strict";
/* ============================================================================================
   Núcleo de la Host Application Sparkplug B (TP3 Integradora | PdCI | UNRaf)
   No usa el DOM ni mqtt.js: recibe (topico, bytes) y publica por una función `publicar`.
   Así se puede probar en Node contra el Edge Node en Python (ver pruebas_t3/test_web.py).

   Bloques (para trazabilidad en el informe):
     H1 - Codificación/decodificación Protobuf del payload Sparkplug B (subconjunto de sparkplug_b.proto)
     H2 - Estado: nodo -> dispositivos -> métricas, con estado en línea / fuera de línea
     H3 - Mensajes recibidos: NBIRTH, NDEATH, NDATA, DBIRTH, DDATA, DDEATH; control de seq; Rebirth
     H4 - Mensajes enviados: STATE (retenido, Will offline), NCMD (Rebirth), DCMD (salidas)
     H5 - Diagnóstico por capa: en qué parte de la cadena está la falla
   ============================================================================================ */
const SPH = (() => {
  // ------------------------------------------------------------------ H1: Protobuf
  const TIPOS = {Int8: 1, Int16: 2, Int32: 3, Int64: 4, UInt8: 5, UInt16: 6, UInt32: 7, UInt64: 8,
                 Float: 9, Double: 10, Boolean: 11, String: 12};
  const NOMBRE_TIPO = Object.fromEntries(Object.entries(TIPOS).map(([k, v]) => [v, k]));
  const CON_SIGNO = {Int8: 8, Int16: 16, Int32: 32, Int64: 64};
  const CAMPO_VALOR = {Int8: 10, Int16: 10, Int32: 10, UInt8: 10, UInt16: 10, UInt32: 10,
                       Int64: 11, UInt64: 11, Float: 12, Double: 13, Boolean: 14, String: 15};
  const utf8 = new TextEncoder(), utf8d = new TextDecoder();

  class Salida {                       // escritor de bytes Protobuf
    constructor() { this.b = []; }
    varint(n) {
      let v = BigInt.asUintN(64, BigInt(n));
      while (v > 0x7Fn) { this.b.push(Number(v & 0x7Fn) | 0x80); v >>= 7n; }
      this.b.push(Number(v));
    }
    tag(campo, tipoWire) { this.varint((campo << 3) | tipoWire); }
    crudo(arr) { for (const x of arr) this.b.push(x); }
    delimitado(campo, arr) { this.tag(campo, 2); this.varint(arr.length); this.crudo(arr); }
    fin() { return Uint8Array.from(this.b); }
  }

  class Entrada {                      // lector de bytes Protobuf
    constructor(u8) { this.u = u8; this.i = 0; }
    fin() { return this.i >= this.u.length; }
    varint() {
      let r = 0n, s = 0n;
      for (;;) {
        if (this.i >= this.u.length) throw new Error("varint truncado");
        const b = this.u[this.i++];
        r |= BigInt(b & 0x7F) << s;
        if (!(b & 0x80)) return r;
        s += 7n;
        if (s > 70n) throw new Error("varint demasiado largo");
      }
    }
    delimitado() {
      const n = Number(this.varint());
      if (this.i + n > this.u.length) throw new Error("campo truncado");
      const x = this.u.subarray(this.i, this.i + n);
      this.i += n;
      return x;
    }
    fijo(n) {
      if (this.i + n > this.u.length) throw new Error("campo truncado");
      const x = new DataView(this.u.buffer, this.u.byteOffset + this.i, n);
      this.i += n;
      return x;
    }
    saltar(wire) {
      if (wire === 0) this.varint();
      else if (wire === 1) this.fijo(8);
      else if (wire === 2) this.delimitado();
      else if (wire === 5) this.fijo(4);
      else throw new Error("wire type no soportado: " + wire);
    }
  }

  function codificarMetrica(m) {
    const o = new Salida();
    o.delimitado(1, utf8.encode(m.nombre));                                   // name
    if (m.timestamp != null) { o.tag(3, 0); o.varint(Math.trunc(m.timestamp)); }  // timestamp
    const tipo = m.tipo;
    if (!(tipo in TIPOS)) throw new Error("tipo Sparkplug desconocido: " + tipo);
    o.tag(4, 0); o.varint(TIPOS[tipo]);                                       // datatype
    if (m.valor == null) { o.tag(7, 0); o.varint(1); return o.fin(); }        // is_null
    const campo = CAMPO_VALOR[tipo];
    if (campo === 10) {                                                       // int_value (uint32)
      o.tag(10, 0);
      o.varint(tipo in CON_SIGNO ? BigInt.asUintN(32, BigInt(Math.trunc(m.valor))) : BigInt(Math.trunc(m.valor)));
    } else if (campo === 11) { o.tag(11, 0); o.varint(BigInt(Math.trunc(m.valor))); }   // long_value
    else if (campo === 12) { const d = new DataView(new ArrayBuffer(4)); d.setFloat32(0, m.valor, true); o.tag(12, 5); o.crudo(new Uint8Array(d.buffer)); }
    else if (campo === 13) { const d = new DataView(new ArrayBuffer(8)); d.setFloat64(0, m.valor, true); o.tag(13, 1); o.crudo(new Uint8Array(d.buffer)); }
    else if (campo === 14) { o.tag(14, 0); o.varint(m.valor ? 1 : 0); }       // boolean_value
    else if (campo === 15) { o.delimitado(15, utf8.encode(String(m.valor))); }// string_value
    return o.fin();
  }

  /** payload = {timestamp, seq, metricas:[{nombre, tipo, valor, timestamp}]} -> Uint8Array */
  function codificar(p) {
    const o = new Salida();
    if (p.timestamp != null) { o.tag(1, 0); o.varint(Math.trunc(p.timestamp)); }
    for (const m of p.metricas || []) o.delimitado(2, codificarMetrica(m));
    if (p.seq != null) {
      if (p.seq < 0 || p.seq > 255) throw new Error("seq fuera de 0..255");
      o.tag(3, 0); o.varint(p.seq);
    }
    return o.fin();
  }

  function decodificarMetrica(u8) {
    const r = new Entrada(u8);
    const m = {nombre: null, tipo: null, valor: null, timestamp: null};
    let datatype = 0, esNulo = false, crudo = null, campoCrudo = null;
    while (!r.fin()) {
      const tag = Number(r.varint()), campo = tag >>> 3, wire = tag & 7;
      if (campo === 1 && wire === 2) m.nombre = utf8d.decode(r.delimitado());
      else if (campo === 3 && wire === 0) m.timestamp = Number(r.varint());
      else if (campo === 4 && wire === 0) datatype = Number(r.varint());
      else if (campo === 7 && wire === 0) esNulo = r.varint() !== 0n;
      else if ((campo === 10 || campo === 11) && wire === 0) { crudo = r.varint(); campoCrudo = campo; }
      else if (campo === 12 && wire === 5) { crudo = r.fijo(4).getFloat32(0, true); campoCrudo = campo; }
      else if (campo === 13 && wire === 1) { crudo = r.fijo(8).getFloat64(0, true); campoCrudo = campo; }
      else if (campo === 14 && wire === 0) { crudo = r.varint() !== 0n; campoCrudo = campo; }
      else if (campo === 15 && wire === 2) { crudo = utf8d.decode(r.delimitado()); campoCrudo = campo; }
      else r.saltar(wire);
    }
    m.tipo = NOMBRE_TIPO[datatype] || ("Desconocido(" + datatype + ")");
    if (esNulo || crudo === null) m.valor = null;
    else if (typeof crudo === "bigint") m.valor = Number(m.tipo in CON_SIGNO ? BigInt.asIntN(CON_SIGNO[m.tipo], crudo) : crudo);
    else m.valor = crudo;
    return m;
  }

  /** Uint8Array -> {timestamp, seq, metricas}. Lanza Error si el payload no es Protobuf válido. */
  function decodificar(u8) {
    const r = new Entrada(u8);
    const p = {timestamp: null, seq: null, metricas: []};
    while (!r.fin()) {
      const tag = Number(r.varint()), campo = tag >>> 3, wire = tag & 7;
      if (campo === 1 && wire === 0) p.timestamp = Number(r.varint());
      else if (campo === 2 && wire === 2) p.metricas.push(decodificarMetrica(r.delimitado()));
      else if (campo === 3 && wire === 0) p.seq = Number(r.varint());
      else r.saltar(wire);
    }
    return p;
  }

  const hex = (u8) => Array.from(u8, (b) => b.toString(16).padStart(2, "0")).join("");
  function volcado(u8) {                 // 16 bytes por línea: offset | hex | ASCII
    const l = [];
    for (let i = 0; i < u8.length; i += 16) {
      const t = u8.subarray(i, i + 16);
      l.push(i.toString(16).padStart(4, "0") + "  " +
             Array.from(t, (b) => b.toString(16).padStart(2, "0")).join(" ").padEnd(47, " ") + "  " +
             Array.from(t, (b) => (b >= 32 && b < 127 ? String.fromCharCode(b) : ".")).join(""));
    }
    return l.join("\n");
  }

  const MENSAJES = ["NBIRTH", "NDEATH", "NDATA", "NCMD", "DBIRTH", "DDEATH", "DDATA", "DCMD"];
  function topico(grupo, mensaje, nodo, dispositivo) {
    return "spBv1.0/" + grupo + "/" + mensaje + "/" + nodo + (dispositivo ? "/" + dispositivo : "");
  }
  function parsearTopico(t) {
    const p = t.split("/");
    if ((p.length !== 4 && p.length !== 5) || p[0] !== "spBv1.0" || !MENSAJES.includes(p[2])) return null;
    return {grupo: p[1], mensaje: p[2], nodo: p[3], dispositivo: p.length === 5 ? p[4] : null};
  }

  // ------------------------------------------------------------------ H2-H5: Host Application
  class Host {
    constructor(cfg) {
      this.cfg = Object.assign({grupo: "UNRaf_PdCI", hostId: "Panel_Web", nodo: "Gateway_OPCUA",
        dispositivo: "Esclavo_Modbus_ID1", minRebirthMs: 5000, timeoutComandoMs: 3000,
        maxMonitor: 300, maxEventos: 200}, cfg || {});
      this.publicar = this.cfg.publicar || (() => {});
      this.ahora = this.cfg.ahora || (() => Date.now());
      this.onCambio = this.cfg.onCambio || (() => {});
      this.brokerOnline = false;
      this.nodos = new Map();           // id -> {estado, nacido, bdSeq, ultimoSeq, ultimoMensaje, metricas, dispositivos}
      this.monitor = [];                // mensajes recibidos y enviados (más nuevo primero)
      this.eventos = [];                // fallas y recuperaciones (más nuevo primero)
      this.pendientes = new Map();      // comandos esperando confirmación
      this._rebirth = new Map();        // nodo -> instante del último pedido
      this._idMonitor = 0;
      this.stats = {mensajes: {}, enviados: {}, saltosSeq: 0, rebirths: 0, errores: 0, comandos: 0,
                    comandosSinConfirmar: 0, reconexiones: 0, bytes: 0};
    }

    // ---- utilidades
    evento(nivel, texto) {
      this.eventos.unshift({t: this.ahora(), nivel, texto});
      if (this.eventos.length > this.cfg.maxEventos) this.eventos.pop();
      this.onCambio();
    }
    _registrar(dir, topico, tipo, bytes, payload, error, texto) {
      this.monitor.unshift({id: ++this._idMonitor, t: this.ahora(), dir, topico, tipo, seq: payload ? payload.seq : null,
        metricas: payload ? payload.metricas : [], bytes, error: error || null, texto: texto || null});
      if (this.monitor.length > this.cfg.maxMonitor) this.monitor.pop();
    }
    nodo(id) {
      if (!this.nodos.has(id)) {
        this.nodos.set(id, {id, estado: "nuevo", nacido: false, bdSeq: null, ultimoSeq: null,
          ultimoMensaje: null, metricas: new Map(), dispositivos: new Map()});
      }
      return this.nodos.get(id);
    }
    dispositivo(nodo, id) {
      if (!nodo.dispositivos.has(id)) nodo.dispositivos.set(id, {id, estado: "nuevo", metricas: new Map(), ultimoCambio: null});
      return nodo.dispositivos.get(id);
    }
    _aplicar(mapa, metricas, reemplazar) {
      if (reemplazar) mapa.clear();
      const cambios = [];
      for (const m of metricas) {
        if (m.nombre == null) continue;
        const antes = mapa.get(m.nombre);
        const tipo = m.tipo && !m.tipo.startsWith("Desconocido") ? m.tipo : (antes ? antes.tipo : m.tipo);
        mapa.set(m.nombre, {valor: m.valor, tipo, timestamp: m.timestamp});
        if (antes && antes.valor !== m.valor) cambios.push([m.nombre, antes.valor, m.valor]);
      }
      return cambios;
    }

    // ---- H4: mensajes que publica la Host Application
    topicoSuscripcion() { return "spBv1.0/" + this.cfg.grupo + "/#"; }
    topicoEstado() { return "spBv1.0/STATE/" + this.cfg.hostId; }
    cargaEstado(online) { return JSON.stringify({online, timestamp: this.ahora()}); }
    will() { return {topic: this.topicoEstado(), payload: this.cargaEstado(false), qos: 1, retain: true}; }

    _enviar(topico, tipo, bytes, payload, opciones) {
      this.publicar(topico, bytes, opciones);
      this.stats.enviados[tipo] = (this.stats.enviados[tipo] || 0) + 1;
      this._registrar("TX", topico, tipo, bytes, payload);
    }

    /** Llamar al terminar la suscripción: publica STATE online (retenido) y pide Rebirth del nodo. */
    alConectar(esReconexion) {
      this.brokerOnline = true;
      if (esReconexion) this.stats.reconexiones++;
      for (const n of this.nodos.values()) { n.nacido = false; n.ultimoSeq = null; }   // se pudieron perder mensajes
      const carga = new TextEncoder().encode(this.cargaEstado(true));
      this.publicar(this.topicoEstado(), carga, {qos: 1, retain: true});
      this.stats.enviados.STATE = (this.stats.enviados.STATE || 0) + 1;
      this._registrar("TX", this.topicoEstado(), "STATE", carga, null, null, new TextDecoder().decode(carga));
      this.evento("ok", esReconexion ? "Reconectado al broker" : "Conectado al broker");
      this._rebirth.clear();
      this.pedirRebirth(this.cfg.nodo, "conexión de la Host Application");
    }
    alDesconectar() {
      if (!this.brokerOnline) return;
      this.brokerOnline = false;
      for (const n of this.nodos.values()) {
        n.nacido = false; n.ultimoSeq = null;
        if (n.estado === "online") n.estado = "desconocido";
        for (const d of n.dispositivos.values()) if (d.estado === "online") d.estado = "desconocido";
      }
      this.evento("error", "Se perdió la conexión con el broker");
    }

    /** NCMD con Node Control/Rebirth = true. Se limita a un pedido cada minRebirthMs por nodo. */
    pedirRebirth(nodoId, motivo, forzar) {
      if (!this.brokerOnline) return false;
      const t = this.ahora(), ultimo = this._rebirth.get(nodoId);
      if (!forzar && ultimo != null && t - ultimo < this.cfg.minRebirthMs) return false;
      this._rebirth.set(nodoId, t);
      const payload = {timestamp: t, seq: null,
        metricas: [{nombre: "Node Control/Rebirth", tipo: "Boolean", valor: true, timestamp: t}]};
      this._enviar(topico(this.cfg.grupo, "NCMD", nodoId), "NCMD", codificar(payload), payload, {qos: 0, retain: false});
      this.stats.rebirths++;
      this.evento("info", "Rebirth solicitado a " + nodoId + " (" + motivo + ")");
      return true;
    }

    /** DCMD para una salida del dispositivo. Devuelve {ok, motivo}. No cambia el valor mostrado:
        el valor real llega después en un DDATA. */
    enviarComando(nombre, valor) {
      const nodo = this.nodos.get(this.cfg.nodo);
      const dev = nodo && nodo.dispositivos.get(this.cfg.dispositivo);
      if (!this.brokerOnline) return {ok: false, motivo: "sin conexión con el broker"};
      if (!nodo || nodo.estado !== "online") return {ok: false, motivo: "el Edge Node está fuera de línea"};
      if (!dev || dev.estado !== "online") return {ok: false, motivo: "el dispositivo está fuera de línea"};
      if (!nombre.startsWith("Outputs/") || !dev.metricas.has(nombre)) return {ok: false, motivo: "no es una salida del dispositivo"};
      const tipo = dev.metricas.get(nombre).tipo;
      const v = tipo === "Boolean" ? !!valor : Math.round(Number(valor));
      const t = this.ahora();
      const payload = {timestamp: t, seq: null, metricas: [{nombre, tipo, valor: v, timestamp: t}]};
      this._enviar(topico(this.cfg.grupo, "DCMD", nodo.id, dev.id), "DCMD", codificar(payload), payload, {qos: 0, retain: false});
      this.pendientes.set(nombre, {valor: v, t});
      this.stats.comandos++;
      this.onCambio();
      return {ok: true, valor: v};
    }

    /** null | "pendiente": estado del último comando enviado sobre una salida. */
    estadoComando(nombre) { return this.pendientes.has(nombre) ? "pendiente" : null; }

    /** Llamar cada ~500 ms: da por no confirmados los comandos que no se reflejaron en un DDATA. */
    tick() {
      const t = this.ahora();
      for (const [nombre, p] of Array.from(this.pendientes)) {
        if (t - p.t >= this.cfg.timeoutComandoMs) {
          this.pendientes.delete(nombre);
          this.stats.comandosSinConfirmar++;
          const real = this._valorDispositivo(nombre);
          this.evento("warn", "Comando " + nombre + " = " + p.valor + " sin confirmar (valor real: " + (real == null ? "?" : real) + ")");
        }
      }
    }
    _valorDispositivo(nombre) {
      const nodo = this.nodos.get(this.cfg.nodo), dev = nodo && nodo.dispositivos.get(this.cfg.dispositivo);
      const m = dev && dev.metricas.get(nombre);
      return m ? m.valor : null;
    }

    // ---- H3: mensajes recibidos
    alMensaje(topicoRx, bytes) {
      const t = parsearTopico(topicoRx);
      if (!t) {                                            // p. ej. spBv1.0/STATE/<otro host>
        if (topicoRx.startsWith("spBv1.0/STATE/")) {
          this._registrar("RX", topicoRx, "STATE", bytes, null, null, new TextDecoder().decode(bytes));
        }
        return;
      }
      if (t.grupo !== this.cfg.grupo) return;
      this.stats.bytes += bytes.length;
      this.stats.mensajes[t.mensaje] = (this.stats.mensajes[t.mensaje] || 0) + 1;
      let p;
      try { p = decodificar(bytes); }
      catch (e) {
        this.stats.errores++;
        this._registrar("RX", topicoRx, t.mensaje, bytes, null, "payload inválido: " + e.message);
        this.evento("error", "Payload Protobuf inválido en " + topicoRx + " (" + e.message + ")");
        return;
      }
      this._registrar("RX", topicoRx, t.mensaje, bytes, p);
      if (t.mensaje === "NCMD" || t.mensaje === "DCMD") return;   // eco de nuestros propios comandos
      const nodo = this.nodo(t.nodo);
      nodo.ultimoMensaje = this.ahora();
      switch (t.mensaje) {
        case "NBIRTH": this._nbirth(nodo, p); break;
        case "NDEATH": this._ndeath(nodo, p); break;
        case "NDATA": if (this._validarSeq(nodo, p, "NDATA")) this._ndata(nodo, p); break;
        case "DBIRTH": if (this._validarSeq(nodo, p, "DBIRTH")) this._dbirth(nodo, t.dispositivo, p); break;
        case "DDATA": if (this._validarSeq(nodo, p, "DDATA")) this._ddata(nodo, t.dispositivo, p); break;
        case "DDEATH": if (this._validarSeq(nodo, p, "DDEATH")) this._ddeath(nodo, t.dispositivo); break;
      }
      this.onCambio();
    }

    /** seq: debe seguir al anterior (módulo 256). Datos sin NBIRTH o un salto -> Rebirth. */
    _validarSeq(nodo, p, tipo) {
      if (!nodo.nacido) {
        this.pedirRebirth(nodo.id, tipo + " sin NBIRTH previo");
        return false;
      }
      if (p.seq == null) { this.evento("warn", tipo + " sin seq"); return true; }
      const esperado = (nodo.ultimoSeq + 1) % 256;
      if (p.seq !== esperado) {
        this.stats.saltosSeq++;
        this.evento("error", "Salto de seq en " + nodo.id + ": esperado " + esperado + ", recibido " + p.seq + " (mensajes perdidos)");
        this.pedirRebirth(nodo.id, "salto de seq");
      }
      nodo.ultimoSeq = p.seq;
      return true;
    }

    _nbirth(nodo, p) {
      const habiaCaido = nodo.estado === "offline" || nodo.estado === "desconocido";
      const bd = p.metricas.find((m) => m.nombre === "bdSeq");
      nodo.estado = "online"; nodo.nacido = true;
      nodo.bdSeq = bd ? bd.valor : null;
      nodo.ultimoSeq = p.seq == null ? 0 : p.seq;
      if (p.seq !== 0) this.evento("warn", "NBIRTH con seq=" + p.seq + " (debería ser 0)");
      this._aplicar(nodo.metricas, p.metricas, true);
      for (const d of nodo.dispositivos.values()) d.estado = "offline";     // hasta que llegue su DBIRTH
      this._rebirth.delete(nodo.id);
      this.evento("ok", "NBIRTH: " + nodo.id + " en línea (bdSeq=" + nodo.bdSeq + ")" + (habiaCaido ? " - recuperado" : ""));
    }
    _ndeath(nodo, p) {
      const bd = p.metricas.find((m) => m.nombre === "bdSeq");
      if (nodo.bdSeq != null && bd && bd.valor !== nodo.bdSeq) {            // NDEATH de una sesión anterior
        this.evento("info", "NDEATH viejo ignorado (bdSeq " + bd.valor + " ≠ " + nodo.bdSeq + ")");
        return;
      }
      nodo.estado = "offline"; nodo.nacido = false; nodo.ultimoSeq = null;
      for (const d of nodo.dispositivos.values()) d.estado = "offline";
      this.evento("error", "NDEATH: " + nodo.id + " fuera de línea (se cayó el Dispositivo 3 o su enlace a internet)");
    }
    _ndata(nodo, p) {
      const antes = {modbus: this._m(nodo, "Status/MODBUS/Estado"), opc: this._m(nodo, "Status/OPCUA/Estado")};
      this._aplicar(nodo.metricas, p.metricas, false);
      const despues = {modbus: this._m(nodo, "Status/MODBUS/Estado"), opc: this._m(nodo, "Status/OPCUA/Estado")};
      if (antes.modbus !== despues.modbus && despues.modbus != null)
        this.evento(despues.modbus === "DONE" ? "ok" : (despues.modbus === "CRC ERROR" ? "warn" : "error"), "Modbus: " + despues.modbus);
      if (antes.opc !== despues.opc && despues.opc != null)
        this.evento(despues.opc === "DONE" ? "ok" : "error", "OPC UA: " + despues.opc);
    }
    _m(nodo, nombre) { const m = nodo.metricas.get(nombre); return m ? m.valor : null; }
    _dbirth(nodo, devId, p) {
      const d = this.dispositivo(nodo, devId);
      const caido = d.estado === "offline";
      this._aplicar(d.metricas, p.metricas, true);
      d.estado = "online"; d.ultimoCambio = this.ahora();
      this.evento("ok", "DBIRTH: " + devId + " en línea" + (caido ? " - recuperado" : ""));
    }
    _ddata(nodo, devId, p) {
      const d = nodo.dispositivos.get(devId);
      if (!d || d.estado !== "online") { this.pedirRebirth(nodo.id, "DDATA sin DBIRTH previo"); return; }
      const cambios = this._aplicar(d.metricas, p.metricas, false);
      if (cambios.length) d.ultimoCambio = this.ahora();
      for (const m of p.metricas) {                                            // confirma comandos pendientes
        const pend = this.pendientes.get(m.nombre);
        if (pend && pend.valor === m.valor) this.pendientes.delete(m.nombre);
      }
    }
    _ddeath(nodo, devId) {
      const d = this.dispositivo(nodo, devId);
      d.estado = "offline";
      const opc = this._m(nodo, "Status/OPCUA/Estado"), mb = this._m(nodo, "Status/MODBUS/Estado");
      this.evento("error", "DDEATH: " + devId + " fuera de línea (" + (opc === "ERROR"
        ? "enlace OPC UA caído" : (mb === "DEVICE NOT REACHED" ? "el esclavo Modbus no responde" : "causa en el estado de las capas")) + ")");
    }

    // ---- H5: diagnóstico por capa
    diagnostico() {
      const nodo = this.nodos.get(this.cfg.nodo);
      const dev = nodo && nodo.dispositivos.get(this.cfg.dispositivo);
      const capas = [];
      const capa = (id, titulo, estado, texto) => capas.push({id, titulo, estado, texto});

      // 1. Esta página <-> broker
      capa("web", "Esta página ⇄ Broker", this.brokerOnline ? "ok" : "error",
        this.brokerOnline ? "Conectada al broker" : "Sin conexión con el broker: revisar Internet de este dispositivo, el broker o las credenciales");

      // 2. Edge Node (dispositivo 3) <-> broker
      let nodoOk = false;
      if (!this.brokerOnline) capa("nodo", "Edge Node (Disp. 3) ⇄ Broker", "desconocido", "Sin datos: esta página no está conectada");
      else if (!nodo || nodo.estado === "nuevo") capa("nodo", "Edge Node (Disp. 3) ⇄ Broker", "desconocido", "Esperando NBIRTH del nodo " + this.cfg.nodo);
      else if (nodo.estado === "online") { nodoOk = true; capa("nodo", "Edge Node (Disp. 3) ⇄ Broker", "ok", "En línea (NBIRTH recibido, bdSeq " + nodo.bdSeq + ")"); }
      else capa("nodo", "Edge Node (Disp. 3) ⇄ Broker", "error", "Fuera de línea (NDEATH / Last Will): se cayó el Dispositivo 3 o su enlace a Internet");

      // 3. OPC UA (dispositivo 3 <-> dispositivo 2)
      const opc = nodoOk ? this._m(nodo, "Status/OPCUA/Estado") : null;
      let opcOk = false;
      if (!nodoOk) capa("opc", "OPC UA (Disp. 3 ⇄ Disp. 2)", "desconocido", "Depende del Edge Node");
      else if (opc === "DONE") { opcOk = true; capa("opc", "OPC UA (Disp. 3 ⇄ Disp. 2)", "ok", "DONE: sesión OPC UA activa"); }
      else if (opc === "ERROR") capa("opc", "OPC UA (Disp. 3 ⇄ Disp. 2)", "error", "ERROR: sin comunicación con el servidor OPC UA (Dispositivo 2: apagado, red o seguridad)");
      else capa("opc", "OPC UA (Disp. 3 ⇄ Disp. 2)", "desconocido", "Sin estado todavía");

      // 4. Modbus (dispositivo 2 <-> dispositivo 1)
      const mb = opcOk ? this._m(nodo, "Status/MODBUS/Estado") : null;
      if (!opcOk) capa("modbus", "Modbus RS485 (Disp. 2 ⇄ Disp. 1)", "desconocido", "Depende del enlace OPC UA");
      else if (mb === "DONE") capa("modbus", "Modbus RS485 (Disp. 2 ⇄ Disp. 1)", "ok", "DONE: el esclavo responde");
      else if (mb === "CRC ERROR") capa("modbus", "Modbus RS485 (Disp. 2 ⇄ Disp. 1)", "warn", "CRC ERROR: tramas corruptas en el bus (ruido, cableado o baudrate)");
      else if (mb === "DEVICE NOT REACHED") capa("modbus", "Modbus RS485 (Disp. 2 ⇄ Disp. 1)", "error", "DEVICE NOT REACHED: el esclavo no responde (cable RS485, alimentación, ID o velocidad)");
      else capa("modbus", "Modbus RS485 (Disp. 2 ⇄ Disp. 1)", "desconocido", "Sin estado todavía");

      const primera = capas.find((c) => c.estado === "error") || capas.find((c) => c.estado === "warn");
      const todasOk = capas.every((c) => c.estado === "ok");
      let falla = null, nivel = "ok";
      if (primera) { falla = primera.titulo + ": " + primera.texto; nivel = primera.estado; }
      else if (!todasOk) { const d = capas.find((c) => c.estado === "desconocido"); falla = d.titulo + ": " + d.texto; nivel = "desconocido"; }
      return {capas, falla, nivel, dispositivoEnLinea: !!(dev && dev.estado === "online")};
    }

    /** Estadísticas de todas las capas para la consigna 1.c. */
    estadisticas() {
      const nodo = this.nodos.get(this.cfg.nodo);
      const v = (n) => (nodo ? this._m(nodo, n) : null);
      return {
        modbus: {estado: v("Status/MODBUS/Estado"), ok: v("Status/MODBUS/OK"), crc: v("Status/MODBUS/CRC_ERROR"), timeout: v("Status/MODBUS/TIMEOUT")},
        opcua: {estado: v("Status/OPCUA/Estado")},
        mqtt: {gatewayReconexiones: v("Status/MQTT/Reconexiones"), webReconexiones: this.stats.reconexiones, bytes: this.stats.bytes},
        sparkplug: {bdSeq: nodo ? nodo.bdSeq : null, ultimoSeq: nodo ? nodo.ultimoSeq : null,
          rebirthsNodo: v("Status/Sparkplug/Rebirths"), rebirthsPedidos: this.stats.rebirths, saltosSeq: this.stats.saltosSeq,
          errores: this.stats.errores, mensajes: Object.assign({}, this.stats.mensajes), comandos: this.stats.comandos,
          sinConfirmar: this.stats.comandosSinConfirmar},
      };
    }
  }

  return {TIPOS, codificar, decodificar, hex, volcado, topico, parsearTopico, Host};
})();
if (typeof module !== "undefined" && module.exports) module.exports = SPH;
