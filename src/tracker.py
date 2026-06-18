"""
tracker.py
----------
El tracker es el unico componente "central", y es DELIBERADAMENTE ligero: NO
transfiere los datos del archivo (por eso sobrevive con "recursos limitados").
Solo hace dos cosas:

    1) Entrega el manifiesto (hashes + metadatos) a quien se une.
    2) Mantiene la lista de pares vivos y se las reparte entre ellos.

A partir de ahi, los nodos se las arreglan solos: el plano de datos es 100% P2P.

Protocolo del tracker: JSON delimitado por saltos de linea sobre TCP.
El cliente abre conexion, envia UNA peticion JSON terminada en '\n', recibe UNA
respuesta JSON terminada en '\n' y cierra.

Peticiones:
    {"op": "get_manifest"}
    {"op": "announce", "node_id": "...", "port": 6881, "event": "started|completed|stopped", "have": <int>}
    {"op": "get_peers", "node_id": "..."}

Uso:
    python3 tracker.py manifest.json --host 0.0.0.0 --port 9000
"""

import argparse
import asyncio
import json
import time

from manifest import load_manifest

PEER_TTL = 60  # segundos sin reanunciar tras los cuales damos por muerto a un par


class Tracker:
    def __init__(self, manifest: dict):
        self.manifest = manifest
        # node_id -> {"host", "port", "last_seen", "have", "completed"}
        self.peers: dict[str, dict] = {}

    def _purge(self):
        """Elimina pares que llevan demasiado tiempo sin reanunciar."""
        now = time.time()
        muertos = [nid for nid, p in self.peers.items()
                   if now - p["last_seen"] > PEER_TTL]
        for nid in muertos:
            del self.peers[nid]

    def handle(self, req: dict, client_host: str) -> dict:
        op = req.get("op")

        if op == "get_manifest":
            return {"ok": True, "manifest": self.manifest}

        if op == "announce":
            nid = req["node_id"]
            event = req.get("event", "update")
            if event == "stopped":
                self.peers.pop(nid, None)
                return {"ok": True}
            self.peers[nid] = {
                "host": client_host,            # IP real vista por el tracker
                "port": int(req["port"]),       # puerto donde el nodo escucha
                "last_seen": time.time(),
                "have": int(req.get("have", 0)),
                "completed": event == "completed",
            }
            self._purge()
            return {"ok": True, "num_peers": len(self.peers)}

        if op == "get_peers":
            self._purge()
            yo = req.get("node_id")
            lista = [
                {"node_id": nid, "host": p["host"], "port": p["port"], "have": p["have"]}
                for nid, p in self.peers.items() if nid != yo
            ]
            return {"ok": True, "peers": lista, "num_pieces": self.manifest["num_pieces"]}

        return {"ok": False, "error": f"op desconocida: {op}"}

    async def _conn(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = writer.get_extra_info("peername")
        client_host = peer[0] if peer else "0.0.0.0"
        try:
            line = await reader.readline()
            if not line:
                return
            req = json.loads(line.decode("utf-8"))
            resp = self.handle(req, client_host)
        except Exception as e:  # noqa: BLE001 (tracker debe ser tolerante)
            resp = {"ok": False, "error": str(e)}
        writer.write((json.dumps(resp) + "\n").encode("utf-8"))
        try:
            await writer.drain()
        finally:
            writer.close()

    async def run(self, host: str, port: int):
        server = await asyncio.start_server(self._conn, host, port,limit=16*1024*1024)
        addr = ", ".join(str(s.getsockname()) for s in server.sockets)
        print(f"[tracker] escuchando en {addr}")
        print(f"[tracker] archivo='{self.manifest['file_name']}' "
              f"piezas={self.manifest['num_pieces']} file_id={self.manifest['file_id']}")
        # Reporte periodico del estado del enjambre (util para el informe)
        asyncio.create_task(self._status_loop())
        async with server:
            await server.serve_forever()

    async def _status_loop(self):
        while True:
            await asyncio.sleep(5)
            self._purge()
            total = self.manifest["num_pieces"]
            completos = sum(1 for p in self.peers.values() if p["completed"])
            if self.peers:
                avg = sum(p["have"] for p in self.peers.values()) / len(self.peers)
                print(f"[tracker] pares={len(self.peers)} completos={completos} "
                      f"progreso_medio={avg/total*100:0.1f}%")


def main():
    ap = argparse.ArgumentParser(description="Tracker P2P (coordinador ligero)")
    ap.add_argument("manifest")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=9000)
    args = ap.parse_args()

    tracker = Tracker(load_manifest(args.manifest))
    try:
        asyncio.run(tracker.run(args.host, args.port))
    except KeyboardInterrupt:
        print("\n[tracker] detenido")


if __name__ == "__main__":
    main()
