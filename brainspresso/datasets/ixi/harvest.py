import json
from pathlib import Path

import aiohttp

from brainspresso.download import CHUNK_SIZE, IfExists
from brainspresso.sources.base import (
    DownloadOptions,
    LabelMap,
    LocalSource,
    RemoteEntry,
    Source,
    SourceUnavailable,
    StaticURLs,
    Wants,
    subject_selection,
)
from brainspresso.sources.credentials import XnatAuth
from brainspresso.sources.xnat_listing import list_project_files

# ----------------------------------------------------------------------
#   NITRC-IR (XNAT project "ixi"), the current host
# ----------------------------------------------------------------------
NITRC_SERVER = 'https://www.nitrc.org/ir'
NITRC_PROJECT = 'ixi'

# XNAT scan ID -> brainspresso key
SCAN2KEY = {
    'T1': 'T1w', 'T2': 'T2w', 'PD': 'PDw', 'MRA': 'angio', 'DTI': 'dwi',
}

# ----------------------------------------------------------------------
#   Original host (Imperial College), now offline
# ----------------------------------------------------------------------
URLBASE = 'https://biomedic.doc.ic.ac.uk/brain-development/downloads/IXI'
URLS = {
    'meta': [f'{URLBASE}/IXI.xls'],
    'T1w': [f'{URLBASE}/IXI-T1.tar'],
    'T2w': [f'{URLBASE}/IXI-T2.tar'],
    'PDw': [f'{URLBASE}/IXI-PD.tar'],
    'angio': [f'{URLBASE}/IXI-MRA.tar'],
    'dwi': [
        f'{URLBASE}/IXI-DTI.tar',
        f'{URLBASE}/bvecs.txt',
        f'{URLBASE}/bvals.txt',
    ],
}
IMAGES = ("T1w", "T2w", "PDw", "angio", "dwi")



def experiment_subject(label: str) -> int | None:
    """'12_HH' -> 12"""
    head = label.split('_')[0]
    return int(head) if head.isdigit() else None


def _label(experiment: str) -> str | None:
    sub = experiment_subject(experiment)
    return None if sub is None else f"{sub:03d}"


def roast_ixi(ctx):
    """Roaster of the IXI sources (imported on use: needs xlrd)"""
    from brainspresso.datasets.ixi.roaster import (  # noqa: PLC0415
        roast_ixi,  # lazy: optional dependency (xlrd)
    )
    return roast_ixi(ctx)


class NitrcIR(Source):
    """
    IXI images from NITRC-IR (XNAT project "ixi", no login needed), in
    `sourcedata/IXI-{T1,T2,PD,MRA,DTI}/`, with the file names of the
    original tarballs, and the b-values/vectors of each subject as
    `IXI-DTI/<IXI###-Site-####>-{bvals,bvecs}.txt`. The experiments
    (scan dates) are saved in `sourcedata/nitrc_experiments.json`.

    Per-run options: `user`, `password` (only if anonymous access is
    refused).
    """

    name = "nitrc"
    provides = frozenset(IMAGES)
    labels = LabelMap(_label)
    roaster = staticmethod(roast_ixi)
    server: str = NITRC_SERVER
    project: str = NITRC_PROJECT
    user: str | None = None
    password: str | None = None

    def __init__(self, server: str | None = None,
                 project: str | None = None, **kwargs):
        super().__init__(**kwargs)
        if server is not None:
            self.server = server
        if project is not None:
            self.project = project

    def auth(self):
        if not self.user:
            return None
        return XnatAuth(self.server, self.user, self.password or "",
                        password_required=False)

    def list(self, selection=None, keys=None) -> list[RemoteEntry]:
        wants = keys if callable(keys) else Wants(keys)
        basic = aiohttp.BasicAuth(self.user, self.password or "") \
            if self.user else None
        try:
            experiments, files = list_project_files(
                self.server, self.project,
                lambda exp: self.keeps_subject(selection,
                                               exp.get("label", "")),
                auth=basic,
            )
        except aiohttp.ClientResponseError as e:
            if e.status in (401, 403):
                raise PermissionError(
                    f"{self.server}: access denied ({e.status}); give "
                    f"credentials with `--user`/`--password`"
                ) from e
            raise SourceUnavailable(f"{self.server}: {e}") from e
        except (aiohttp.ClientConnectionError, OSError) as e:
            raise SourceUnavailable(f"{self.server}: {e}") from e
        # Session metadata (scan date) for reference
        out = [RemoteEntry("nitrc_experiments.json", meta={
            "generated": True,
            "content": json.dumps(
                {e["label"]: {"ID": e["ID"], "date": e.get("date")}
                 for e in experiments}, indent=2),
        })]

        # Subject prefix (IXI###-Site-####) of each experiment, from its
        # image file names, used to name its b-values/vectors.
        prefixes = {}
        for f in files:
            m = f.meta
            if m["collection"] == 'NIfTI' and f.name.startswith('IXI'):
                prefixes.setdefault(m["experiment"],
                                    '-'.join(f.name.split('-')[:3]))

        for f in files:
            m = f.meta
            key = SCAN2KEY.get(m["scan"])
            if key is None or not wants(key):
                continue
            folder = f"IXI-{m['scan']}"
            if m["collection"] == 'NIfTI':
                local = f"{folder}/{f.name}"
            elif m["collection"] in ('bval', 'bvec'):
                prefix = prefixes.get(m["experiment"], m["experiment"])
                local = f"{folder}/{prefix}-{m['collection']}s.txt"
            else:       # SNAPSHOTS (QC thumbnails)
                continue
            f.meta["local"] = local
            out.append(f)
        return out

    def local_path(self, entry: RemoteEntry) -> str:
        return entry.meta.get("local", entry.path)


class Imperial(StaticURLs):
    """Original IXI tarballs and `IXI.xls` (brain-development.org;
    offline)"""

    name = "imperial"
    urls = URLS
    roaster = staticmethod(roast_ixi)


def sources() -> dict:
    """Sources of IXI"""
    return {
        "nitrc": NitrcIR(),
        "imperial": Imperial(),
        # IXI.xls shipped with brainspresso (or put in sourcedata/)
        "bundled": LocalSource(provides={"meta"}, name="bundled"),
    }


DEFAULTS = {**{key: ["nitrc", "imperial"] for key in IMAGES},
            "meta": ["bundled"]}


def nitrc_downloaders(
    src: Path,
    *,
    keys: set[str],
    subs: set[int] = frozenset(),
    exclude_subs: set[int] = frozenset(),
    user: str | None = None,
    password: str | None = None,
    ifexists: IfExists.Choice = "skip",
    chunk_size: int = CHUNK_SIZE,
    server: str = NITRC_SERVER,
    project: str = NITRC_PROJECT,
):
    """Downloaders of the IXI files hosted on NITRC-IR, into the source
    folder `src`"""
    source = NitrcIR(server, project).configure(user=user,
                                                password=password)
    options = DownloadOptions(ifexists=ifexists, chunk_size=chunk_size)
    entries = source.list(subject_selection(subs, exclude_subs, width=3),
                          Wants(keys))
    return source.downloaders(entries, src, options)
