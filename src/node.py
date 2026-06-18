"""
node.py
-------
Un nodo del enjambre. Cada nodo es a la vez CLIENTE (descarga piezas) y
SERVIDOR (sube piezas a otros). Implementa los tres mecanismos de la estrategia:

  1. SELECCION DE PIEZAS
        - Random-First : la PRIMERA pieza se pide al azar (arranque instantaneo).
        - Rarest-First : el resto se pide empezando por la pieza menos comun
                         en el enjambre (maximiza diversidad, mata el cuello final).

  2. SELECCION DE PARES (configurable con --policy)
        - coop : DESBLOQUEO COOPERATIVO. Subimos al que MENOS piezas tiene
                 (al mas rezagado) y rotamos. Optimo para un enjambre de aliados
                 porque minimiza el makespan (que el ULTIMO nodo termine pronto).
        - tft  : TIT-FOR-TAT clasico de BitTorrent. Subimos a quien mas nos sube.
                 Incluido para comparar en la Fase 3 y demostrar por que 'coop' gana.
        En ambos: limite de slots de subida (control de congestion) + un slot
        "optimista" rotatorio para que los nodos nuevos arranquen.

  3. INTEGRIDAD
        - Cada pieza recibida se verifica con SHA-256 contra el manifiesto.
          Si no coincide (corrupcion por corte de red), se descarta y se vuelve
          a pedir. Las piezas validas se guardan en disco (reanudacion).

Uso:
    # Nodo origen (tiene el archivo completo):
    python3 node.py --tracker 127.0.0.1:9000 --port 6881 --seed --data archivo.bin

    # Estudiante (descarga):
    python3 node.py --tracker 127.0.0.1:9000 --port 6882 --out descarga.bin --policy coop
"""

import argparse
import asyncio
import json
import os
import random
import time
import uuid
import socket

import protocol as P
from manifest import piece_bounds, verify_piece, load_manifest

# ----------------------- parametros sintonizables ------------------------
UNCHOKE_INTERVAL = 2.0       # cada cuanto recalculamos a quien desbloqueamos
ANNOUNCE_INTERVAL = 8.0      # cada cuanto reanunciamos / redescubrimos pares
REQUEST_TIMEOUT = 15.0       # si una pieza pedida no llega, la re-pedimos
SCHEDULER_TICK = 0.2         # frecuencia del planificador de descargas
MAX_UNCHOKE = 4              # slots de subida simultaneos (incl. 1 optimista)
MAX_INFLIGHT_PER_PEER = 4    # piezas pedidas a la vez a un mismo par
ENDGAME_THRESHOLD = 3        # al quedar <= N piezas, permitir pedidos duplicados


class PeerConn:
    """Estado de la conexion con un par concreto."""
    def __init__(self, node_id, host, port, reader, writer):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.reader = reader
        self.writer = writer
        self.bitfield = None            # piezas que ESTE par posee
        self.they_choking_me = True     # arranca estrangulandome
        self.am_choking_them = True     # arranco estrangulandolo
        self.am_interested = False      # le he avisado que me interesa?
        self.peer_interested = False    # le intereso yo a el?
        self.inflight = set()           # piezas que le he pedido y no han llegado
        self.bytes_recv = 0             # bytes recibidos en la ventana (para TFT)
        self.alive = True
        self.wlock = asyncio.Lock()     # evita entrelazar tramas al escribir


class Node:
    def __init__(self, args):
        self.args = args
        self.node_id = args.node_id or uuid.uuid4().hex[:12]
        self.tracker_host, self.tracker_port = args.tracker.split(":")
        self.tracker_port = int(self.tracker_port)
        self.listen_host = args.host
        self.listen_port = args.port
        self.policy = args.policy
        self.is_seed = bool(args.seed)

        self.manifest = None
        self.num_pieces = 0
        self.piece_size = 0
        self.file_size = 0

        self.have = None                # nuestro bitfield
        self.have_count = 0
        self.peers: dict[str, PeerConn] = {}
        self.inflight_global: dict[int, tuple] = {}  # idx -> (node_id, t_pedido)

        # archivo de datos: el seed lee del original; el leecher escribe/lee el out
        self.data_path = args.data if self.is_seed else args.out
        self.fh = None

        self.start_time = None
        self.finish_time = None
        self.complete_event = asyncio.Event()

        # limitador de subida (emula uplink limitado / "recursos limitados")
        self.up_bps = (args.up_kbps or 0) * 1024
        self._uplock = asyncio.Lock()
        self._upload_clock = 0.0

    # ----------------------- cliente del tracker -------------------------
    async def tracker_request(self, req: dict) -> dict:
        reader, writer = await asyncio.open_connection(self.tracker_host, self.tracker_port,limit=16*1024*1024)
        try:
            writer.write((json.dumps(req) + "\n").encode())
            await writer.drain()
            line = await reader.readline()
            return json.loads(line.decode())
        finally:
            writer.close()

    async def announce(self, event="update"):
        try:
            resp = await self.tracker_request({
                "op": "announce", "node_id": self.node_id,
                "port": self.listen_port, "event": event, "have": self.have_count,
            })
            return resp
        except Exception as e:  # noqa: BLE001
            print(f"[{self.node_id}] aviso al tracker fallo: {e}")
            return {"ok": False}

    # --------------------------- preparacion -----------------------------
    async def bootstrap(self):
        resp = await self.tracker_request({"op": "get_manifest"})
        self.manifest = resp["manifest"]
        self.num_pieces = self.manifest["num_pieces"]
        self.piece_size = self.manifest["piece_size"]
        self.file_size = self.manifest["file_size"]

        if self.is_seed:
            self.have = P.full_bitfield(self.num_pieces)
            self.have_count = self.num_pieces
            self.fh = open(self.data_path, "rb")
            self.complete_event.set()
            self.finish_time = 0.0
        else:
            self.have = P.new_bitfield(self.num_pieces)
            self._prepare_output_file()

    def _prepare_output_file(self):
        """Crea/abre el archivo de salida y reanuda piezas previas si existen."""
        bits_path = self.data_path + ".bits"
        if (os.path.exists(self.data_path)
                and os.path.getsize(self.data_path) == self.file_size
                and os.path.exists(bits_path)):
            with open(bits_path, "rb") as bf:
                self.have = bytearray(bf.read())
            self.have_count = P.count_bits(self.have)
            self.fh = open(self.data_path, "r+b")
            print(f"[{self.node_id}] reanudando: {self.have_count}/{self.num_pieces} piezas")
        else:
            self.fh = open(self.data_path, "wb")
            self.fh.truncate(self.file_size)   # preasigna el espacio
            self.fh.close()
            self.fh = open(self.data_path, "r+b")
        if self.have_count == self.num_pieces:
            self.complete_event.set()

    def _save_bits(self):
        with open(self.data_path + ".bits", "wb") as bf:
            bf.write(bytes(self.have))

    def read_piece(self, index: int) -> bytes:
        offset, length = piece_bounds(self.manifest, index)
        self.fh.seek(offset)
        return self.fh.read(length)

    def write_piece(self, index: int, data: bytes):
        offset, _ = piece_bounds(self.manifest, index)
        self.fh.seek(offset)
        self.fh.write(data)
        self.fh.flush()

    # ---------------------- envio de mensajes ----------------------------
    async def send(self, peer: PeerConn, msg_type: int, payload: bytes = b""):
        if not peer.alive:
            return
        try:
            async with peer.wlock:
                peer.writer.write(P.encode_message(msg_type, payload))
                await peer.writer.drain()
        except Exception:  # noqa: BLE001
            await self.drop_peer(peer)

    async def broadcast_have(self, index: int):
        """Anuncia a TODOS los pares que ya tenemos una pieza nueva."""
        payload = P.pack_u32(index)
        for peer in list(self.peers.values()):
            await self.send(peer, P.HAVE, payload)

    # ------------------------ conexiones ---------------------------------
    async def serve(self):
        server = await asyncio.start_server(
            self._handle_incoming, self.listen_host, self.listen_port)
        print(f"[{self.node_id}] escuchando pares en "
              f"{self.listen_host}:{self.listen_port} (seed={self.is_seed}, policy={self.policy})")
        async with server:
            await server.serve_forever()

    #async def _handle_incoming(self, reader, writer):
    #    try:
    #        mtype, payload = await P.read_message(reader)
    #        if mtype != P.HANDSHAKE:
    #            writer.close(); return
    #        info = json.loads(payload.decode())
    #        if info.get("file_id") != self.manifest["file_id"]:
    #            writer.close(); return
    #        their_id = info["node_id"]
    #        # responde con nuestro handshake + bitfield
    #        writer.write(P.encode_message(P.HANDSHAKE, json.dumps(
    #            {"node_id": self.node_id, "file_id": self.manifest["file_id"]}).encode()))
    #        writer.write(P.encode_message(P.BITFIELD, bytes(self.have)))
    #        await writer.drain()
    #        host, port = writer.get_extra_info("peername")[:2]
    #        await self._register_peer(their_id, host, port, reader, writer)
    #    except Exception:  # noqa: BLE001
    #        writer.close()

    async def _handle_incoming(self, reader, writer):
        try:
            # OPTIMIZACIÓN: Ampliar los buffers de red del socket para conexiones entrantes
            sock = writer.get_extra_info("peername")
            raw_socket = writer.get_extra_info("socket")
            if raw_socket:
                raw_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024) # 1MB
                raw_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024) # 1MB

            mtype, payload = await P.read_message(reader)
            if mtype != P.HANDSHAKE:
                writer.close(); return
            info = json.loads(payload.decode())
            if info.get("file_id") != self.manifest["file_id"]:
                writer.close(); return
            their_id = info["node_id"]
            
            writer.write(P.encode_message(P.HANDSHAKE, json.dumps(
                {"node_id": self.node_id, "file_id": self.manifest["file_id"]}).encode()))
            writer.write(P.encode_message(P.BITFIELD, bytes(self.have)))
            await writer.drain()
            host, port = writer.get_extra_info("peername")[:2]
            await self._register_peer(their_id, host, port, reader, writer)
        except Exception:  # noqa: BLE001
            writer.close()


    #async def connect(self, host, port):
    #    try:
    #        reader, writer = await asyncio.open_connection(host, port)
    #        writer.write(P.encode_message(P.HANDSHAKE, json.dumps(
    #            {"node_id": self.node_id, "file_id": self.manifest["file_id"]}).encode()))
    #        await writer.drain()
    #        mtype, payload = await P.read_message(reader)
    #        if mtype != P.HANDSHAKE:
    #            writer.close(); return
    #        info = json.loads(payload.decode())
    #        their_id = info["node_id"]
    #        writer.write(P.encode_message(P.BITFIELD, bytes(self.have)))
    #        await writer.drain()
    #        await self._register_peer(their_id, host, port, reader, writer)
    #    except Exception:  # noqa: BLE001
    #        pass


    async def connect(self, host, port):
        try:
            # OPTIMIZACIÓN: Pasamos el limit de 16MB aquí también para evitar cuellos de botella
            reader, writer = await asyncio.open_connection(host, port, limit=16 * 1024 * 1024)
            
            # OPTIMIZACIÓN: Ampliar los buffers de red del socket para conexiones salientes
            raw_socket = writer.get_extra_info("socket")
            if raw_socket:
                raw_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
                raw_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024)

            writer.write(P.encode_message(P.HANDSHAKE, json.dumps(
                {"node_id": self.node_id, "file_id": self.manifest["file_id"]}).encode()))
            await writer.drain()
            mtype, payload = await P.read_message(reader)
            if mtype != P.HANDSHAKE:
                writer.close(); return
            info = json.loads(payload.decode())
            their_id = info["node_id"]
            writer.write(P.encode_message(P.BITFIELD, bytes(self.have)))
            await writer.drain()
            await self._register_peer(their_id, host, port, reader, writer)
        except Exception:  # noqa: BLE001
            pass

    async def _register_peer(self, their_id, host, port, reader, writer):
        if their_id == self.node_id or their_id in self.peers:
            writer.close(); return       # evita auto-conexion y duplicados
        peer = PeerConn(their_id, host, port, reader, writer)
        self.peers[their_id] = peer
        asyncio.create_task(self._reader_loop(peer))

    async def drop_peer(self, peer: PeerConn):
        if not peer.alive:
            return
        peer.alive = False
        # re-encola las piezas que le habiamos pedido
        for idx in list(peer.inflight):
            self.inflight_global.pop(idx, None)
        self.peers.pop(peer.node_id, None)
        try:
            peer.writer.close()
        except Exception:  # noqa: BLE001
            pass

    # --------------------- recepcion de mensajes -------------------------
    async def _reader_loop(self, peer: PeerConn):
        try:
            while peer.alive:
                mtype, payload = await P.read_message(peer.reader)
                await self._dispatch(peer, mtype, payload)
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        except Exception:  # noqa: BLE001
            pass
        finally:
            await self.drop_peer(peer)

    async def _dispatch(self, peer: PeerConn, mtype: int, payload: bytes):
        if mtype == P.BITFIELD:
            peer.bitfield = bytearray(payload)
            self._update_interest(peer)
            await self._sync_interest(peer)
        elif mtype == P.HAVE:
            idx = P.unpack_u32(payload)
            if peer.bitfield is None:
                peer.bitfield = P.new_bitfield(self.num_pieces)
            P.set_bit(peer.bitfield, idx)
            self._update_interest(peer)
            await self._sync_interest(peer)
        elif mtype == P.INTERESTED:
            peer.peer_interested = True
        elif mtype == P.NOT_INTERESTED:
            peer.peer_interested = False
        elif mtype == P.CHOKE:
            peer.they_choking_me = True
            for idx in list(peer.inflight):     # re-encola lo pedido
                peer.inflight.discard(idx)
                self.inflight_global.pop(idx, None)
        elif mtype == P.UNCHOKE:
            peer.they_choking_me = False
        elif mtype == P.REQUEST:
            await self._serve_request(peer, P.unpack_u32(payload))
        elif mtype == P.PIECE:
            await self._on_piece(peer, payload)
        # KEEPALIVE: nada

    async def _serve_request(self, peer: PeerConn, index: int):
        # solo servimos si lo tenemos y NO lo estamos estrangulando
        if peer.am_choking_them or not P.has_bit(self.have, index):
            return
        # se envia en tarea aparte para no bloquear el bucle lector con el throttle
        asyncio.create_task(self._do_serve(peer, index))

    async def _throttle_upload(self, nbytes: int):
        """Espacia los envios para que la subida agregada no supere up_bps."""
        if not self.up_bps:
            return
        async with self._uplock:
            now = time.monotonic()
            self._upload_clock = max(self._upload_clock, now) + nbytes / self.up_bps
            delay = self._upload_clock - now
        if delay > 0:
            await asyncio.sleep(delay)

    async def _do_serve(self, peer: PeerConn, index: int):
        if not peer.alive or peer.am_choking_them or not P.has_bit(self.have, index):
            return
        data = self.read_piece(index)
        await self._throttle_upload(len(data))
        await self.send(peer, P.PIECE, P.pack_u32(index) + data)

    async def _on_piece(self, peer: PeerConn, payload: bytes):
        index = P.unpack_u32(payload)
        data = payload[4:]
        peer.bytes_recv += len(data)
        peer.inflight.discard(index)
        # ya la teniamos (caso endgame): ignorar
        if P.has_bit(self.have, index):
            self.inflight_global.pop(index, None)
            return
        # VERIFICACION DE INTEGRIDAD (SHA-256)
        if not verify_piece(self.manifest, index, data):
            print(f"[{self.node_id}] pieza {index} CORRUPTA -> descartada y re-pedida")
            self.inflight_global.pop(index, None)
            return
        # pieza valida: guardar
        self.write_piece(index, data)
        P.set_bit(self.have, index)
        self.have_count += 1
        self._save_bits()
        self.inflight_global.pop(index, None)
        await self.broadcast_have(index)
        if self.have_count == self.num_pieces:
            await self._on_complete()

    async def _on_complete(self):
        self.finish_time = time.time()
        elapsed = self.finish_time - self.start_time

        # verificacion del archivo completo
        self.fh.flush()
        
        import hashlib
        self.fh.seek(0)
        h = hashlib.sha256()
        while True:
            chunk = self.fh.read(1 << 20)
            if not chunk:
                break
            h.update(chunk)
        ok = h.hexdigest() == self.manifest["file_hash"]
        print(f"[{self.node_id}] COMPLETO en {elapsed:0.2f}s  "
              f"(hash global {'OK' if ok else 'FALLO'})  -> ahora soy SEED")
        
        
        self.is_seed = True
        await self.announce("completed")
        self.complete_event.set()

    # --------------------- interes (interested) --------------------------
    def _peer_has_something_i_need(self, peer: PeerConn) -> bool:
        if peer.bitfield is None:
            return False
        for i in range(self.num_pieces):
            if not P.has_bit(self.have, i) and P.has_bit(peer.bitfield, i):
                return True
        return False

    def _update_interest(self, peer: PeerConn):
        peer._want = self._peer_has_something_i_need(peer)

    async def _sync_interest(self, peer: PeerConn):
        want = getattr(peer, "_want", False)
        if want and not peer.am_interested:
            peer.am_interested = True
            await self.send(peer, P.INTERESTED)
        elif not want and peer.am_interested:
            peer.am_interested = False
            await self.send(peer, P.NOT_INTERESTED)

    # --------------------- bucles principales ----------------------------
    async def fetch_peers(self):
        """Pide al tracker la lista de pares y abre conexiones nuevas.

        Regla anti-duplicados: solo MARCAMOS (conexion saliente) a los pares cuyo
        node_id es mayor que el nuestro; del resto esperamos su conexion entrante.
        Asi cada par de nodos abre exactamente UNA conexion (full-duplex)."""
        try:
            resp = await self.tracker_request({"op": "get_peers", "node_id": self.node_id})
        except Exception:  # noqa: BLE001
            return
        if not resp.get("ok"):
            return
        for p in resp.get("peers", []):
            pid = p["node_id"]
            if pid == self.node_id or pid in self.peers:
                continue
            if self.node_id < pid:
                asyncio.create_task(self.connect(p["host"], p["port"]))

    async def announce_loop(self):
        while True:
            await self.announce("update")
            await self.fetch_peers()
            await asyncio.sleep(ANNOUNCE_INTERVAL)

    async def unchoke_manager(self):
        """Decide a quien desbloqueamos cada UNCHOKE_INTERVAL."""
        while True:
            interested = [p for p in self.peers.values() if p.peer_interested and p.alive]

            if self.policy == "tft":
                # TIT-FOR-TAT: prioriza a quien mas nos ha subido en la ventana
                interested.sort(key=lambda p: p.bytes_recv, reverse=True)
            else:
                # COOPERATIVO: prioriza al mas rezagado (menos piezas) -> minimiza makespan
                interested.sort(key=lambda p: P.count_bits(p.bitfield) if p.bitfield else 0)

            elegidos = set(id(p) for p in interested[:MAX_UNCHOKE - 1])
            # slot optimista / altruista: un par al azar fuera de los elegidos
            resto = [p for p in interested if id(p) not in elegidos]
            if resto:
                elegidos.add(id(random.choice(resto)))

            for peer in self.peers.values():
                debe_desbloquear = id(peer) in elegidos
                if debe_desbloquear and peer.am_choking_them:
                    peer.am_choking_them = False
                    await self.send(peer, P.UNCHOKE)
                elif not debe_desbloquear and not peer.am_choking_them:
                    peer.am_choking_them = True
                    await self.send(peer, P.CHOKE)

            # reinicia la ventana de contabilidad para TFT
            for peer in self.peers.values():
                peer.bytes_recv = 0

            await asyncio.sleep(UNCHOKE_INTERVAL)

    async def download_scheduler(self):
        """Elige que pieza pedir y a quien (Random-First -> Rarest-First)."""
        while not self.complete_event.is_set():
            await asyncio.sleep(SCHEDULER_TICK)
            self._expire_timeouts()
            if self.have_count >= self.num_pieces:
                break

            faltan = P.missing_pieces(self.have, self.num_pieces)
            if not faltan:
                continue
            endgame = len(faltan) <= ENDGAME_THRESHOLD

            # candidatas: piezas que algun par (que NO me estrangula) posee
            disponibilidad = {}   # idx -> [pares que la tienen y me han desbloqueado]
            for idx in faltan:
                if not endgame and idx in self.inflight_global:
                    continue
                portadores = [p for p in self.peers.values()
                              if p.alive and not p.they_choking_me
                              and p.bitfield is not None and P.has_bit(p.bitfield, idx)]
                if portadores:
                    disponibilidad[idx] = portadores
            if not disponibilidad:
                continue

            if self.have_count == 0:
                # RANDOM-FIRST: la primera pieza, al azar (arranque instantaneo)
                orden = list(disponibilidad.keys())
                random.shuffle(orden)
            else:
                # RAREST-FIRST: primero las menos comunes (desempate al azar)
                orden = sorted(disponibilidad.keys(),
                               key=lambda i: (len(disponibilidad[i]), random.random()))

            for idx in orden:
                if not endgame and idx in self.inflight_global:
                    continue
                portadores = [p for p in disponibilidad[idx]
                              if len(p.inflight) < MAX_INFLIGHT_PER_PEER]
                if not portadores:
                    continue
                # pide al portador menos cargado (reparte la demanda)
                peer = min(portadores, key=lambda p: len(p.inflight))
                peer.inflight.add(idx)
                if not endgame:
                    self.inflight_global[idx] = (peer.node_id, time.time())
                await self.send(peer, P.REQUEST, P.pack_u32(idx))

    def _expire_timeouts(self):
        ahora = time.time()
        vencidas = [idx for idx, (_, t) in self.inflight_global.items()
                    if ahora - t > REQUEST_TIMEOUT]
        for idx in vencidas:
            nid, _ = self.inflight_global.pop(idx)
            peer = self.peers.get(nid)
            if peer:
                peer.inflight.discard(idx)

    # ------------------------------ run ----------------------------------
    async def run(self):
        await self.bootstrap()
        self.start_time = time.time()
        await self.announce("started")
        await self.fetch_peers()      # conexion inmediata, sin esperar el ciclo
        tasks = [
            asyncio.create_task(self.serve()),
            asyncio.create_task(self.announce_loop()),
            asyncio.create_task(self.unchoke_manager()),
        ]
        if not self.is_seed:
            tasks.append(asyncio.create_task(self.download_scheduler()))
        # los nodos siguen vivos sirviendo aunque ya hayan terminado (siembra)
        await asyncio.gather(*tasks)


def main():
    ap = argparse.ArgumentParser(description="Nodo del enjambre P2P")
    ap.add_argument("--tracker", required=True, help="host:puerto del tracker")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, required=True, help="puerto donde escucho pares")
    ap.add_argument("--node-id", default=None)
    ap.add_argument("--policy", choices=["coop", "tft"], default="coop")
    ap.add_argument("--seed", action="store_true", help="soy el nodo origen")
    ap.add_argument("--data", help="ruta del archivo COMPLETO (solo seed)")
    ap.add_argument("--out", help="ruta de salida de la descarga (solo leecher)")
    ap.add_argument("--up-kbps", type=int, default=0,
                    help="limite de subida en KB/s (0 = sin limite). Emula uplink limitado.")
    args = ap.parse_args()

    if args.seed and not args.data:
        ap.error("--seed requiere --data archivo_completo")
    if not args.seed and not args.out:
        ap.error("un leecher requiere --out archivo_salida")

    node = Node(args)
    try:
        asyncio.run(node.run())
    except KeyboardInterrupt:
        print(f"\n[{node.node_id}] detenido")


if __name__ == "__main__":
    main()
