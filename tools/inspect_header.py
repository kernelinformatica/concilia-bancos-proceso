#!/usr/bin/env python3
"""Inspecciona la cabecera de un archivo y clasifica si parece .xls (OLE BIFF), .xlsx (PK zip), texto UTF-16, XML, o otro.
Uso: python tools/inspect_header.py <ruta_al_archivo>
"""
import sys
from pathlib import Path

HEX_DISPLAY = 64


def hexdump_prefix(data: bytes, length: int = HEX_DISPLAY) -> str:
    h = ' '.join(f"{b:02x}" for b in data[:length])
    try:
        raw = data[:256].decode('latin-1', errors='replace')
        s = ''.join((ch if (32 <= ord(ch) <= 126 or ch in '\n\r\t') else '.') for ch in raw)
    except Exception:
        s = repr(data[:256])
    return f"hex={h} snippet={s}"


def classify(data: bytes) -> str:
    if len(data) >= 8 and data[:8] == b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1':
        return 'xls (OLE BIFF)'
    if len(data) >= 4 and data[:4] == b'PK\x03\x04':
        return 'xlsx (ZIP PK)'
    if data.startswith(b'<?xml') or b'<?xml' in data[:64]:
        return 'xml / Excel 2003 XML'
    nulls = data.count(b'\x00')
    if nulls > (len(data) // 4):
        return 'probable UTF-16 / texto con NULs'
    # crude check for csv-like (commas/tabs and ascii digits)
    snippet = data[:256]
    if any(c in snippet for c in b',;|\t'):
        try:
            snippet.decode('utf-8')
            return 'text-like (utf-8) - posible CSV'
        except Exception:
            return 'text-like (no UTF-8) - posible CSV/legacy encoding'
    return 'desconocido/otro'


def main():
    if len(sys.argv) < 2:
        print('Uso: python inspect_header.py <archivo>')
        sys.exit(2)
    p = Path(sys.argv[1])
    if not p.exists():
        print('No existe:', p)
        sys.exit(2)
    data = p.read_bytes()[:1024]
    print('Ruta:', p)
    print('Tamaño (bytes, cabezera leída):', p.stat().st_size, len(data))
    print('Clasificación:', classify(data))
    print('HeXDUMP/Snippet:')
    print(hexdump_prefix(data, length=HEX_DISPLAY))


if __name__ == '__main__':
    main()

