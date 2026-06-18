"""
manifest.py
-----------
El "manifiesto" es el equivalente al archivo .torrent: describe el archivo a
distribuir SIN contener sus datos. Incluye:

    - nombre y tamano del archivo
    - tamano de pieza (piece_size)
    - numero de piezas
    - el hash SHA-256 de CADA pieza   <-- esto da la robustez ante intermitencias
    - el hash SHA-256 del archivo completo (verificacion final)

El tracker entrega este manifiesto a cada nodo al unirse. Cuando un nodo recibe
una pieza de un companero, recalcula su SHA-256 y lo compara contra el del
manifiesto: si coincide la guarda; si no, la descarta y la vuelve a pedir.

Uso por linea de comandos:
    python3 manifest.py crear  archivo.bin  manifest.json  --piece-size 2097152
"""

import argparse
import hashlib
import json
import os


def build_manifest(file_path: str, piece_size: int) -> dict:
    """Lee el archivo, lo parte logicamente en piezas y calcula los hashes."""
    file_size = os.path.getsize(file_path)
    num_pieces = (file_size + piece_size - 1) // piece_size
    piece_hashes = []
    file_hasher = hashlib.sha256()

    # Buffer de lectura en disco optimizado (ej. 64KB o el tamaño de pieza si es menor)
    buffer_size = min(65536, piece_size)

    with open(file_path, "rb") as f:
        for _ in range(num_pieces):
            # Creamos un bytearray vacío con el tamaño exacto de la pieza
            # Nota: La última pieza puede ser más corta
            bytes_left = min(piece_size, file_size - f.tell())
            piece_buffer = bytearray(bytes_left)

            # Usamos memoryview para llenar el bytearray directamente desde el archivo
            mv = memoryview(piece_buffer)
            offset = 0

            while offset < bytes_left:
                chunk_to_read = min(buffer_size, bytes_left - offset)
                # Leemos directo al segmento correspondiente de la memoria congelada
                f.readinto(mv[offset:offset + chunk_to_read])
                offset += chunk_to_read

            # Pasamos la vista de memoria directamente a los hashers (Cero copias en RAM)
            piece_hashes.append(hashlib.sha256(mv).hexdigest())
            file_hasher.update(mv)


            #chunk = f.read(piece_size)
            #piece_hashes.append(hashlib.sha256(chunk).hexdigest())
            #file_hasher.update(chunk)

    return {
        "file_name": os.path.basename(file_path),
        "file_size": file_size,
        "piece_size": piece_size,
        "num_pieces": num_pieces,
        "piece_hashes": piece_hashes,
        "file_hash": file_hasher.hexdigest(),
        # file_id identifica al "torrent": dos manifiestos del mismo archivo coinciden.
        "file_id": file_hasher.hexdigest()[:16],
    }


def load_manifest(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_manifest(manifest: dict, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)


def piece_bounds(manifest: dict, index: int):
    """Devuelve (offset, longitud) de la pieza `index` dentro del archivo.
    La ultima pieza puede ser mas corta."""
    piece_size = manifest["piece_size"]
    file_size = manifest["file_size"]
    offset = index * piece_size
    length = min(piece_size, file_size - offset)
    return offset, length


def verify_piece(manifest: dict, index: int, data: bytes) -> bool:
    """True si el SHA-256 de `data` coincide con el del manifiesto para la pieza."""
    return hashlib.sha256(data).hexdigest() == manifest["piece_hashes"][index]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generador de manifiesto P2P")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("crear", help="Crear manifiesto a partir de un archivo")
    p.add_argument("archivo")
    p.add_argument("salida")
    p.add_argument("--piece-size", type=int, default=2 * 1024 * 1024,
                   help="Tamano de pieza en bytes (default 2 MiB)")

    args = parser.parse_args()
    if args.cmd == "crear":
        m = build_manifest(args.archivo, args.piece_size)
        save_manifest(m, args.salida)
        print(f"Manifiesto creado: {args.salida}")
        print(f"  archivo     : {m['file_name']}")
        print(f"  tamano      : {m['file_size']:,} bytes")
        print(f"  pieza       : {m['piece_size']:,} bytes")
        print(f"  num_piezas  : {m['num_pieces']}")
        print(f"  file_id     : {m['file_id']}")
