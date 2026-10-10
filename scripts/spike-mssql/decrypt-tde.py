"""Lab-only: unwrap the TDE certificate exported by tde-dump.sql and decrypt one page.

Reads scripts/spike-mssql/out/. Does not print the private key or the DEK.
The private-key password is TDE_PVK_PASSWORD, from the environment or from
local/lab.env beside this script (not committed; see lab.env.example).
"""
import hashlib
import os
import pathlib
import struct
import sys

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

HERE = pathlib.Path(__file__).resolve().parent
OUT = HERE / "out"


def lab_value(name: str) -> str:
    value = os.environ.get(name)
    env = HERE / "local" / "lab.env"
    if not value and env.exists():
        for line in env.read_text().splitlines():
            key, _, rest = line.partition("=")
            if key.strip() == name:
                value = rest.strip()
    if not value:
        raise SystemExit(f"set {name}, or copy lab.env.example to local/lab.env")
    return value


PASSWORD = lab_value("TDE_PVK_PASSWORD")
PAGE_ID = 360
MARKER = "CESTREAM_TDE_MARKER_7f3a".encode("utf-16le")


def rc4(key: bytes, data: bytes) -> bytes:
    s = list(range(256))
    j = 0
    for i in range(256):
        j = (j + s[i] + key[i % len(key)]) & 255
        s[i], s[j] = s[j], s[i]
    i = j = 0
    out = bytearray()
    for b in data:
        i = (i + 1) & 255
        j = (j + s[i]) & 255
        s[i], s[j] = s[j], s[i]
        out.append(b ^ s[(s[i] + s[j]) & 255])
    return bytes(out)


def load_private_key(pvk: bytes, password: str):
    magic, _reserved, _keytype, encrypted, saltlen, keylen = struct.unpack_from("<6I", pvk)
    if magic != 0xB0B5F11E:
        raise SystemExit(f"pvk magic {magic:#x}")
    salt = pvk[24 : 24 + saltlen]
    blob = pvk[24 + saltlen : 24 + saltlen + keylen]
    if encrypted:
        # PVK leaves the 8-byte BLOBHEADER in the clear. RC4 covers the rest.
        # Strong key is SHA1(salt || password)[:16]. The weak retry zeros bytes 5..15.
        plain = None
        for raw in (password.encode("ascii"), password.encode("utf-16le")):
            digest = hashlib.sha1(salt + raw).digest()
            strong = digest[:16]
            weak = bytearray(digest[:16])
            weak[5:] = b"\x00" * 11
            for key in (strong, bytes(weak)):
                body = rc4(key, blob[8:])
                candidate = blob[:8] + body
                magic = struct.unpack_from("<I", candidate, 8)[0]
                if magic == 0x32415352:
                    plain = candidate
                    break
            if plain is not None:
                break
        if plain is None:
            raise SystemExit("pvk password did not unwrap")
    else:
        plain = blob
    btype, _ver, _res, _alg = struct.unpack_from("<BBHI", plain)
    if btype != 0x07:
        raise SystemExit(f"blob type {btype:#x}")
    magic, bitlen, pubexp = struct.unpack_from("<III", plain, 8)
    if magic != 0x32415352:
        raise SystemExit(f"rsa magic {magic:#x}")
    cb = bitlen // 8
    off = 20

    def take(n):
        nonlocal off
        raw = plain[off : off + n]
        off += n
        return int.from_bytes(raw, "little")

    n = take(cb)
    take(cb // 2)
    take(cb // 2)
    take(cb // 2)
    take(cb // 2)
    take(cb // 2)
    d = take(cb)
    return n, d, pubexp, cb


def pkcs1_unpad(em: bytes):
    if len(em) < 11 or em[0:2] != b"\x00\x02":
        return None
    sep = em.find(b"\x00", 2)
    if sep < 10:
        return None
    return em[sep + 1 :]


def rsa_decrypt(n, d, k, ct: bytes):
    if len(ct) != k:
        return None
    m = pow(int.from_bytes(ct, "big"), d, n)
    em = m.to_bytes(k, "big")
    return pkcs1_unpad(em)


def parse_dbcc_page(text: str, page_id: int) -> bytes:
    marker = f"PAGE: (1:{page_id})"
    start = text.find(marker)
    if start < 0:
        raise SystemExit(f"missing {marker}")
    dump = text.find("Memory Dump @", start)
    nxt = text.find("DBCC execution completed", dump)
    body = bytearray()
    for line in text[dump:nxt].splitlines():
        if ":" not in line:
            continue
        rest = line.split(":", 1)[1].strip()
        for word in rest.split():
            if len(word) == 8 and all(c in "0123456789abcdefABCDEF" for c in word):
                body += bytes.fromhex(word)
            else:
                break
    if len(body) < 8192:
        raise SystemExit(f"parsed page is {len(body)} bytes")
    return bytes(body[:8192])


def aes_cbc(key: bytes, iv: bytes, data: bytes) -> bytes:
    dec = Cipher(algorithms.AES(key), modes.CBC(iv)).decryptor()
    return dec.update(data) + dec.finalize()


def main():
    n, d, _e, k = load_private_key((OUT / "ce_stream_tde.pvk").read_bytes(), PASSWORD)
    print(f"certificate rsa_bits={k * 8}")
    mdf = (OUT / "ce_stream_tde.mdf").read_bytes()
    dump = (OUT / "tde-dump.txt").read_text(encoding="utf-8", errors="replace")
    mark = "m_rgbThumbprint = 0x"
    at = dump.find(mark)
    if at < 0:
        raise SystemExit("thumbprint missing from tde-dump.txt")
    thumb = bytes.fromhex(dump[at + len(mark) : at + len(mark) + 40])
    thumb_at = mdf.find(thumb)
    if thumb_at < 0:
        raise SystemExit("thumbprint not on the boot page")
    # Thumbprint, a 4-byte length, then the RSA ciphertext little-endian.
    off = thumb_at + len(thumb) + 4
    pt = rsa_decrypt(n, d, k, mdf[off : off + k][::-1])
    if pt is None or len(pt) < 44 or pt[0] != 0x08:
        raise SystemExit("DEK blob did not unwrap")
    key_len = int.from_bytes(pt[8:12], "little")
    key = pt[12 : 12 + key_len]
    print(f"dek_blob_off={off} aes_key_bits={len(key) * 8}")

    disk = mdf[PAGE_ID * 8192 : (PAGE_ID + 1) * 8192]
    plain_page = parse_dbcc_page(dump, PAGE_ID)
    print(f"disk_has_marker={MARKER in disk}")
    iv = struct.pack("<II", PAGE_ID, 1) + b"\x00" * 8
    full = aes_cbc(key, iv, disk[96:])
    print(f"page_body_match={full == plain_page[96:]}")
    print(f"page_marker={MARKER in full}")

    ldf = (OUT / "ce_stream_tde_log.ldf").read_bytes()
    log_marker = "CESTREAM_TDE_LOG_9c2e".encode("utf-16le")
    print(f"log_has_marker={log_marker in ldf}")
    block_off = 2039808 + 0x3B8 * 512
    body = ldf[block_off + 24 : block_off + 1024]
    body = body[: len(body) // 16 * 16]
    # The row sits past the first AES block, so CBC chaining fixes it.
    got = aes_cbc(key, b"\x00" * 16, body)
    row = bytes.fromhex(
        "30000800020000000200000100390043004500530054005200450041004D"
        "005F005400440045005F004C004F0047005F003900630032006500"
    )
    print(f"log_row_match={got.find(row) >= 0}")


if __name__ == "__main__":
    sys.exit(main())
