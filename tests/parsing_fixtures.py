"""Small, synthetic fixtures; no supplier data or secrets."""
import codecs


def supported_cases():
    text = 'id,value\r\n1,"café, tea"\r\n2,"say ""yes""\r\nnext"\r\n'
    for encoding, bom in (("utf-8", b""), ("utf-8-sig", b""),
                          ("utf-16", b""), ("utf-16-le", codecs.BOM_UTF16_LE),
                          ("utf-16-be", codecs.BOM_UTF16_BE),
                          ("latin-1", b""), ("cp1252", b"")):
        yield encoding, bom + text.encode(encoding), dict(file_encoding=encoding)
    yield "cp1252_punctuation", 'id,value\n1,“€”\n'.encode("cp1252"), dict(file_encoding="cp1252")
    yield "headerless", b'1,"hello, world"\n2,plain\n', dict(header=False)
    yield "backslash", b'id,value\n1,"say \\"yes\\""\n', dict(csv_options={"escape_char": "\\"})
    yield "pipe", b'id|value\n1|"hello|world"\n', dict(delimiter="|")
    yield "zero", b'id,value\n', {}
    yield "nulls", b'id,value\n1,\n2,""\n3,NULL\n4,  \n', {}
    yield "ascii", b'id,value\n1,plain\n', dict(file_encoding="ascii")
