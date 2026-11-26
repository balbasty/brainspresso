#!/usr/bin/env python
"""Fetch header-only NIfTI files from the public FCP-INDI S3 bucket.

Maintainer tool used to give `printouts/<DS>/<site>/nii/*.nii` headers to
conversion-table entries that have none (protocol2bids needs a header to
map phase-encoding / slice directions to voxel axes).

Only the first 64 KiB of the `.nii.gz` is downloaded (HTTP Range request,
anonymous HTTPS, as in `brainspresso/datasets/INDI/make_fcon1000.py`); just
enough is decompressed to read the NIfTI-1/2 header, and the header (plus
extensions up to `vox_offset`) is stored as an uncompressed `.nii` without
data.

Usage
-----
    # list keys under a prefix (relative to data/Projects/)
    python scripts/fetch_nii_headers.py list CORR/RawData/DC_1/ [--grep RE]

    # fetch the header of one key
    python scripts/fetch_nii_headers.py get KEY OUTPUT.nii

KEY is relative to `data/Projects/` (e.g.
`ABIDE2/RawData/ABIDEII-GU_1/28800/session_1/rest_1/rest.nii.gz`).
Record the KEY of each fetched header in `printouts/README.md`.
"""
import argparse
import re
import struct
import sys
import urllib.parse
import urllib.request
import zlib
from pathlib import Path

BUCKET = 'https://s3.amazonaws.com/fcp-indi'
PROJECTS = 'data/Projects/'
CHUNK = 65536


def list_keys(prefix, grep=None, limit=10000):
    """List keys under `data/Projects/<prefix>`."""
    keys, marker = [], ''
    while len(keys) < limit:
        q = urllib.parse.urlencode({'prefix': PROJECTS + prefix,
                                    'marker': marker})
        with urllib.request.urlopen(f'{BUCKET}?{q}') as f:
            xml = f.read().decode()
        page = re.findall(r'<Key>([^<]*)</Key>', xml)
        keys += [k[len(PROJECTS):] for k in page]
        if '<IsTruncated>true</IsTruncated>' not in xml or not page:
            break
        marker = page[-1]
    if grep:
        keys = [k for k in keys if re.search(grep, k)]
    return keys


def fetch_header(key):
    """Return the header-only NIfTI bytes of `data/Projects/<key>`."""
    url = f'{BUCKET}/{PROJECTS}{urllib.parse.quote(key)}'
    req = urllib.request.Request(url, headers={'Range': f'bytes=0-{CHUNK-1}'})
    with urllib.request.urlopen(req) as f:
        raw = f.read()
    if key.endswith('.gz'):
        raw = zlib.decompressobj(16 + zlib.MAX_WBITS).decompress(raw)
    return strip_data(raw)


def strip_data(raw):
    """Keep the header and extensions (up to vox_offset)."""
    for endian in '<>':
        size = struct.unpack(endian + 'i', raw[:4])[0]
        if size == 348:
            offset = struct.unpack(endian + 'f', raw[108:112])[0]
            break
        if size == 540:
            offset = struct.unpack(endian + 'q', raw[168:176])[0]
            break
    else:
        raise ValueError('not a NIfTI file')
    offset = int(offset)
    if len(raw) < offset:
        raise ValueError('header larger than the downloaded chunk')
    return raw[:offset]


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    sub = p.add_subparsers(dest='cmd', required=True)
    ls = sub.add_parser('list')
    ls.add_argument('prefix')
    ls.add_argument('--grep')
    get = sub.add_parser('get')
    get.add_argument('key')
    get.add_argument('output')
    a = p.parse_args(argv)
    if a.cmd == 'list':
        for k in list_keys(a.prefix, a.grep):
            print(k)
        return 0
    out = Path(a.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(fetch_header(a.key))
    print(f'{a.key} -> {out}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
