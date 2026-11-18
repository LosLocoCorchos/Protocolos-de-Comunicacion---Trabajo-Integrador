"""
Puente OPC UA <-> maestro Modbus (dispositivo 2) - TP3 Integradora | PdCI | UNRaf

Esta capa NO depende de asyncua: trabaja con "nodos" que solo necesitan un metodo
`async publicar(valor)`. Asi se puede probar sin un servidor OPC UA y el servidor real
(servidor_opcua_t3.py) solo aporta el pegamento con asyncua.

Bloques (para trazabilidad en el informe):
  P1 - al_ciclo():            tras cada ciclo de polling, publica en los nodos los valores que cambiaron
                              y el estado de la comunicacion Modbus (Estado, contadores, Alcanzable)
  P2 - al_cambiar_nodo():     un cliente OPC UA escribio un nodo de salida -> se encola un comando
  P3 - trabajador_escrituras(): toma los comandos de a uno, los escribe por Modbus (FC05/FC06),
                              relee la variable para confirmar y publica el valor REAL del esclavo

Dos problemas que resuelve:
  * Eco: cuando el propio servidor publica un valor en un nodo, asyncua avisa del cambio igual que
    si lo hubiera escrito un cliente. Se ignora todo valor igual al ultimo que publico el puente;
    si no, cada lectura dispararia una escritura Modbus.
  * Comandos rechazados (excepcion Modbus, esclavo mudo, valor invalido): el nodo ya tiene el valor
    que escribio el cliente, asi que despues de cada comando se vuelve a publicar el valor real.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Dict, Optional

from maestro_modbus import ErrorModbus, Poller

_log = logging.getLogger("puente")

# Valores de los nodos Status.MODBUS.* antes del primer ciclo
ESTADO_INICIAL = {"Estado": "DEVICE NOT REACHED", "OK": 0, "CRC_ERROR": 0, "TIMEOUT": 0,
                  "Alcanzable": False}


class Puente:
    def __init__(self, poller: Poller, nodos_datos: Dict[str, Any], nodos_estado: Dict[str, Any],
                 consola: Optional[Callable[[Dict[str, Any]], None]] = None):
        self.poller = poller
        self.nodos_datos = nodos_datos            # nombre de variable -> nodo con publicar()
        self.nodos_estado = nodos_estado          # "Estado", "OK", ... -> nodo con publicar()
        self.consola = consola
        self._cola: asyncio.Queue = asyncio.Queue()
        self._publicado: Dict[str, Any] = {}      # ultimo valor publicado por el puente en cada nodo
        self._estado_publicado: Dict[str, Any] = dict(ESTADO_INICIAL)

    # ------------------------------------------------------------ publicacion
    async def _publicar_dato(self, nombre: str, valor: Any):
        self._publicado[nombre] = valor           # ANTES de publicar: asyncua puede avisar enseguida
        await self.nodos_datos[nombre].publicar(valor)

    async def _publicar_estado(self, res: Dict[str, Any]):
        c = res["contadores"]
        actual = {"Estado": res["estado"], "OK": c["ok"], "CRC_ERROR": c["crc_error"],
                  "TIMEOUT": c["timeout"], "Alcanzable": bool(res["alcanzable"])}
        for clave, valor in actual.items():
            if self._estado_publicado.get(clave) != valor:      # solo si cambio
                self._estado_publicado[clave] = valor
                await self.nodos_estado[clave].publicar(valor)

    # ------------------------------------------------------------ P1: despues de cada ciclo
    async def al_ciclo(self, res: Dict[str, Any]):
        for nombre, valor in res["cambios"].items():
            await self._publicar_dato(nombre, valor)
        await self._publicar_estado(res)
        if self.consola is not None:
            self.consola(res)

    # ------------------------------------------------------------ P2: escritura de un cliente OPC UA
    def al_cambiar_nodo(self, nombre: str, valor: Any):
        """Llamado (sincrono) por asyncua cuando cambia un nodo de salida."""
        variable = self.poller.por_nombre.get(nombre)
        if variable is None or not variable.escribible:
            return
        if valor == self._publicado.get(nombre):      # eco de una publicacion propia
            return
        _log.info("Comando recibido por OPC UA: %s = %s", nombre, valor)
        self._cola.put_nowait((nombre, valor))

    # ------------------------------------------------------------ P3: comandos hacia el esclavo
    async def trabajador_escrituras(self):
        while True:
            nombre, valor = await self._cola.get()
            try:
                await asyncio.to_thread(self.poller.escribir, nombre, valor)
                cambio = await asyncio.to_thread(self.poller.releer, nombre)
                _log.info("Comando aplicado en el esclavo: %s = %s", nombre, valor)
                for n, v in cambio.items():
                    await self._publicar_dato(n, v)
            except (ErrorModbus, ValueError) as e:
                _log.warning("Comando %s = %s no aplicado: %s", nombre, valor, e)
            finally:
                # Pase lo que pase, el nodo debe mostrar lo que realmente tiene el esclavo
                real = self.poller.valores.get(nombre)
                if real is not None:
                    await self._publicar_dato(nombre, real)

    async def correr(self, intervalo_s: float):
        """Polling + atencion de comandos, en paralelo (corre hasta que se cancele)."""
        await asyncio.gather(self.poller.correr(intervalo_s, self.al_ciclo),
                             self.trabajador_escrituras())
