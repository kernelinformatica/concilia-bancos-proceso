from pathlib import Path

p = Path(__file__).parent / 'docs' / 'marga_credi_diciembre.xls'
print('path:', p)
if not p.exists():
    print('File not found')
    raise SystemExit(1)

b = p.read_bytes()
print('len:', len(b))
print('\n=== first 128 bytes hex ===')
print(' '.join(f"{x:02x}" for x in b[:128]))

print('\n=== first 256 bytes repr snippet (latin1) ===')
try:
    print(b[:256].decode('latin1'))
except Exception as e:
    print('latin1 decode failed:', e)

print('\n=== try UTF-16-LE decode (first 256) ===')
try:
    s = b[:256].decode('utf-16le')
    print('decoded (utf-16le):', s[:500])
except Exception as e:
    print('utf-16le decode failed:', e)

print('\n=== check signatures ===')
sig4 = b[:4]
print('first4 hex:', ' '.join(f"{x:02x}" for x in sig4))
if sig4 == b'PK\x03\x04':
    print('Looks like a ZIP/PK (xlsx)')
if b[:8] == bytes.fromhex('d0cf11e0a1b11ae1'):
    print('Looks like OLE compound (xls older)')

print('\n=== search for common markers ===')
for mark in [b"<?xml", b"Workbook", b"m_ingreso", b"xl/workbook.xml"]:
    idx = b.find(mark)
    print(f"marker {mark!r} -> idx {idx}")

print('\n=== tail bytes (256-512) hex ===')
print(' '.join(f"{x:02x}" for x in b[256:512]))

print('\nDone')

