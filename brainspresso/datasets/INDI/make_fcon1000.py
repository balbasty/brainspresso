"""
Maintainer tool: build the FCON1000 templates from the bucket.

* `templates/fcon1000/_sites.tsv`: site, label code and repetition time.
  The repetition time is read from the NIfTI header of every resting
  state scan (HTTP range requests: only the first 64 KiB of each file
  is downloaded). The most common value of a site is used, and scans
  that differ are listed in `_repetition_times.tsv` (per subject).
* `templates/fcon1000/_participants.tsv`: the site tables merged, with
  harmonised columns (`age`, `sex`, `handedness`), the site and the
  original label. Subjects that have images but no demographics are
  listed too.

Its outputs are committed, so users do not need to run it.

    python -m brainspresso.datasets.INDI.make_fcon1000
"""
from __future__ import annotations

import asyncio
import io
import re
import zlib
from collections import Counter, defaultdict

import nibabel as nib

from brainspresso.datasets.INDI.engine import (
    TEMPLATES,
    format_tsv,
    read_table,
)
from brainspresso.datasets.INDI.fcon1000 import CODES
from brainspresso.download.session import new_session
from brainspresso.sources import fcp

PREFIX = f"{fcp.PROJECTS}/FCON1000/"
OUT = TEMPLATES / "fcon1000"
# column name in the site tables -> harmonised name
COLUMNS = {"handed": "handedness"}
# Codes used for missing values in the site tables
MISSING = {"9999", "-9999"}
# Broken headers: NewYork_a has 4 names for 5 columns. The 4th column
# ranges from 7.88 to 49.16, like the ages of the other sites: it is
# taken to be the age.
HEADERS = {
    "NewYork_a": "participant_id\tnumber\tsex\tage\tpsych_eval",
}


async def read_tr(session, bucket, key: str) -> float | None:
    async with session.get(
        bucket.url(key), headers={"Range": "bytes=0-65535"}
    ) as r:
        raw = await r.read()
    try:
        data = zlib.decompressobj(16 + zlib.MAX_WBITS).decompress(raw)
    except zlib.error:
        data = raw          # not compressed
    header = nib.Nifti1Header.from_fileobj(
        io.BytesIO(data[:348] + b"\0" * 4)
    )
    tr = float(header["pixdim"][4])
    unit = header.get_xyzt_units()[1]
    tr *= {"msec": 1e-3, "usec": 1e-6}.get(unit, 1)
    return round(tr, 4) if tr > 0 else None


async def main():
    bucket = fcp.get_bucket()
    sem = asyncio.Semaphore(32)
    async with new_session() as session:
        bucket.session = session
        objects = [o async for o in bucket.list(PREFIX)]
        bolds = [o.key for o in objects
                 if "/func" in o.key and o.size > 0
                 and not o.key.endswith("/")]

        async def tr_of(key):
            async with sem:
                return key, await read_tr(session, bucket, key)

        trs = dict(await asyncio.gather(*map(tr_of, bolds)))

        async def text(key):
            async with session.get(bucket.url(key)) as r:
                if r.status != 200:
                    return None
                return (await r.read()).decode("latin1")

        tables = {site: await text(f"{PREFIX}{site}/participants.tsv")
                  for site in CODES}

    # --- repetition times -------------------------------------------
    site_tr, exceptions = {}, {}
    for site in CODES:
        values = Counter(v for k, v in trs.items()
                         if k.startswith(f"{PREFIX}{site}/") and v)
        if not values:
            raise ValueError(f"No TR for {site}")
        site_tr[site], _ = values.most_common(1)[0]
        if len(values) > 1:
            print(f"{site} has several TRs: {dict(values)}")
        by_subject = defaultdict(set)
        for key, tr in trs.items():
            m = re.match(rf"{PREFIX}{site}/sourcedata/sub-([^/]+)/", key)
            if m and tr:
                by_subject[m.group(1)].add(tr)
        for label, values in by_subject.items():
            if len(values) > 1:
                raise ValueError(f"{site}/sub-{label}: TRs {values}")
            tr, = values
            if tr != site_tr[site]:
                exceptions[f"sub-{CODES[site]}{label}"] = tr

    # --- subjects with images ---------------------------------------
    imaged = defaultdict(set)
    for o in objects:
        m = re.match(rf"{PREFIX}([^/]+)/(?:sourcedata|derivatives)/"
                     r"sub-([A-Za-z0-9]+)", o.key)
        if m and o.size > 0:
            imaged[m.group(1)].add(m.group(2))

    rows = []
    for site, code in CODES.items():
        listed = {}
        table = tables[site] or "participant_id\n"
        if site in HEADERS:
            table = HEADERS[site] + "\n" + table.split("\n", 1)[1]
        for row in read_table(table):
            label = row.pop("participant_id").strip()
            label = label[4:] if label.startswith("sub-") else label
            row.pop("number", None)
            row = {COLUMNS.get(k, k): ("n/a" if v in MISSING else v)
                   for k, v in row.items() if k}
            if "sex" in row:
                row["sex"] = {"m": "M", "f": "F"}.get(
                    row["sex"].strip().lower(), row["sex"].strip()
                )
            listed[label] = row
        for label in sorted(set(listed) | imaged[site]):
            rows.append({
                "participant_id": f"sub-{code}{label}",
                "site": site,
                "site_participant_id": f"sub-{label}",
                **listed.get(label, {}),
            })
    first = ["participant_id", "site", "site_participant_id",
             "age", "sex", "handedness"]
    (OUT / "_participants.tsv").write_text(format_tsv(rows, first),
                                           encoding="utf-8", newline="\n")
    (OUT / "_sites.tsv").write_text(format_tsv(
        [{"site": s, "code": c, "repetition_time": site_tr[s],
          "participants": sum(r["site"] == s for r in rows)}
         for s, c in CODES.items()],
    ), encoding="utf-8", newline="\n")
    (OUT / "_repetition_times.tsv").write_text(format_tsv(
        [{"participant_id": k, "repetition_time": v}
         for k, v in sorted(exceptions.items())],
        ["participant_id", "repetition_time"],
    ), encoding="utf-8", newline="\n")
    print(f"{len(rows)} participants, {len(CODES)} sites, "
          f"{len(exceptions)} subjects with a different TR")


if __name__ == "__main__":
    asyncio.run(main())
