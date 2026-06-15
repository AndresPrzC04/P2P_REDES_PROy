"""
protocol.py
-----------
Protocolo de comunicacion entre pares del enjambre.

Formato de trama (frame) sobre TCP:

    [ 4 bytes: longitud (big-endian) ][ 1 byte: tipo ][ payload (longitud-1 bytes) ]

La longitud incluye el byte de tipo. Asi una trama vacia (solo tipo) tiene longitud = 1.

Tambien viven aqui las utilidades de "bitfield": un mapa de bits donde el bit i
indica si poseemos (1) o no (0) la pieza i. Es lo que cada par anuncia al conectarse
para que los demas sepan que puede ofrecer.
"""

import asyncio
import struct

# ---------------------------------------------------------------------------
# Tipos de mensaje
# ---------------------------------------------------------------------------
KEEPALIVE = 0       # sin payload. Mantiene viva la conexion.
HANDSHAKE = 1       # payload JSON: {"node_id": str, "file_id": str}
BITFIELD = 2        # payload: bytes crudos del bitfield (1 bit por pieza)
HAVE = 3            # payload: uint32 indice de pieza recien completada
INTERESTED = 4      # sin payload
NOT_INTERESTED = 5  # sin payload
CHOKE = 6           # sin payload. "Te estoy estrangulando: no te subo datos."
UNCHOKE = 7         # sin payload. "Te desbloqueo: pideme piezas."
REQUEST = 8         # payload: uint32 indice de pieza solicitada (pieza completa)
PIECE = 9           # payload: uint32 indice + bytes de la pieza

NAMES = {
    0: "KEEPALIVE", 1: "HANDSHAKE", 2: "BITFIELD", 3: "HAVE",
    4: "INTERESTED", 5: "NOT_INTERESTED", 6: "CHOKE", 7: "UNCHOKE",
    8: "REQUEST", 9: "PIECE",
}

_HEADER = struct.Struct(">I")   # 4 bytes de longitud
_U32 = struct.Struct(">I")      # un entero sin signo de 4 bytes


# ---------------------------------------------------------------------------
# Lectura / escritura de tramas
# ---------------------------------------------------------------------------
async def read_message(reader: asyncio.StreamReader):
    """Lee una trama completa. Devuelve (tipo, payload_bytes).

    Lanza asyncio.IncompleteReadError si el par cerro la conexion."""
    header = await reader.readexactly(4)
    (length,) = _HEADER.unpack(header)
    if length == 0:
        return KEEPALIVE, b""
    body = await reader.readexactly(length)
    msg_type = body[0]
    payload = body[1:]
    return msg_type, payload


def encode_message(msg_type: int, payload: bytes = b"") -> bytes:
    """Construye los bytes de una trama lista para enviar."""
    body = bytes([msg_type]) + payload
    return _HEADER.pack(len(body)) + body


def pack_u32(value: int) -> bytes:
    return _U32.pack(value)


def unpack_u32(data: bytes) -> int:
    return _U32.unpack(data[:4])[0]


# ---------------------------------------------------------------------------
# Utilidades de bitfield
# ---------------------------------------------------------------------------
def new_bitfield(num_pieces: int) -> bytearray:
    """Crea un bitfield vacio (todas las piezas en 0) para num_pieces piezas."""
    return bytearray((num_pieces + 7) // 8)


def full_bitfield(num_pieces: int) -> bytearray:
    """Crea un bitfield con TODAS las piezas en 1 (lo usa el nodo origen/seed)."""
    bf = new_bitfield(num_pieces)
    for i in range(num_pieces):
        set_bit(bf, i)
    return bf


def set_bit(bf: bytearray, index: int) -> None:
    bf[index // 8] |= (1 << (7 - (index % 8)))


def has_bit(bf, index: int) -> bool:
    return bool(bf[index // 8] & (1 << (7 - (index % 8))))


def count_bits(bf) -> int:
    """Cuantas piezas posee este bitfield."""
    return sum(bin(byte).count("1") for byte in bf)


def missing_pieces(bf, num_pieces: int):
    """Indices de las piezas que aun NO tenemos."""
    return [i for i in range(num_pieces) if not has_bit(bf, i)]
