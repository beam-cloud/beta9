import hashlib


DETERMINISTIC_BLOCK_SIZE = 4096


def deterministic_payload_range(nonce: str, label: str, offset: int, length: int) -> bytes:
    out = bytearray()
    position = offset
    while len(out) < length:
        block_index = position // DETERMINISTIC_BLOCK_SIZE
        seed = hashlib.sha256(f"{nonce}:{label}:{block_index}".encode()).digest()
        block = (seed * ((DETERMINISTIC_BLOCK_SIZE // len(seed)) + 1))[
            :DETERMINISTIC_BLOCK_SIZE
        ]
        block_offset = position % DETERMINISTIC_BLOCK_SIZE
        take = min(length - len(out), len(block) - block_offset)
        out.extend(block[block_offset : block_offset + take])
        position += take
    return bytes(out)


def deterministic_sha256(nonce: str, label: str, size: int) -> str:
    hasher = hashlib.sha256()
    offset = 0
    while offset < size:
        length = min(1024 * 1024, size - offset)
        hasher.update(deterministic_payload_range(nonce, label, offset, length))
        offset += length
    return hasher.hexdigest()
