# Protocol printouts

This folder holds the MRI protocol printouts (scanner exports, scan
parameter tables) that brainspresso uses to build the BIDS sidecar
templates of datasets whose providers do not distribute sidecars:

| folder    | dataset  | templates                                |
| --------- | -------- | ---------------------------------------- |
| `ABIDE1/` | ABIDE-I  | `brainspresso/datasets/indi/abide/i/templates` |
| `ABIDE2/` | ABIDE-II | `brainspresso/datasets/indi/abide/ii/templates` |
| `CoRR/`   | CoRR     | `brainspresso/datasets/indi/corr/templates`    |
| `MPI-LEMON/` | INDI MPI-LEMON | `brainspresso/datasets/indi/mpi_lemon/templates` (fill-in) |
| `NKI-RS/` | INDI NKI-RS | `brainspresso/datasets/indi/nki_rs/templates` (fill-in) |
| `SALD/`   | INDI SALD | `brainspresso/datasets/indi/sald/templates` (fill-in) |
| `SLIM/`   | INDI SLIM | `brainspresso/datasets/indi/slim/templates` (fill-in) |

INDI projects host BIDS sidecars in the bucket, but they are often
incomplete (unless dcm2niix wrote them). Their templates are **fill-in**
templates (`fill = true`, see below): they never replace a hosted file.
Until the placement hook (`claude/feat/hosted-bids-placement`) merges
them key by key, a fill-in template is skipped whole when the bucket
hosts a file at its path, and otherwise written at the dataset root,
where BIDS inheritance lets the hosted, lower-level sidecars win.

It lives **outside** the `brainspresso` package and is never shipped in
the wheel/sdist. The printouts are converted once, by a maintainer, with
[protocol2bids](https://github.com/balbasty/protocol2bids); the generated
sidecars are committed into the templates. protocol2bids is **not** a
runtime dependency: it is listed in `requirements-maint.txt` (repository
root, pinned to the commit recorded in the locks), not in the package
metadata, which PyPI would reject with a git URL.

## Layout

```
printouts/<DATASET>/
    <SITE>_<SAMPLE>/           # e.g. ABIDE2/BNI_1, CoRR/BMB_1, ABIDE1/UCLA_1
        *.pdf, *.txt           # printouts
        nii/*.nii              # NIfTI headers only (data stripped), used to
                               # map anatomical directions to voxel axes
    sidecars.toml              # conversion table (printout -> sidecar)
    sidecars.lock.json         # provenance of the committed sidecars
    report.md                  # last regeneration report (diff per sidecar)
```

## Provenance and license

The printouts are public scan-parameter documents published by the dataset
providers (FCP/INDI) to document their acquisitions:

- ABIDE-I: <https://fcon_1000.projects.nitrc.org/indi/abide/abide_I.html>
  (scan parameters linked per site)
- ABIDE-II: <https://fcon_1000.projects.nitrc.org/indi/abide/abide_II.html>,
  files under <https://fcon_1000.projects.nitrc.org/indi/abide/scan_params/>
- CoRR: <https://fcon_1000.projects.nitrc.org/indi/CoRR/html/>,
  files under
  <https://fcon_1000.projects.nitrc.org/indi/CoRR/html/_static/scan_parameters/>

They remain the property of their authors and are redistributed here
unchanged, for documentation and reproducibility only. The ABIDE/CoRR
imaging data themselves are subject to the providers' terms of use.

Files come from two sources:

- **brainspresso**: printouts previously stored in
  `brainspresso/datasets/{ABIDE/II,CoRR}/templates/PDFs/` (now
  `datasets/indi/{abide/ii,corr}/`; downloaded by the
  old, since removed, `*_parse_scan_param.py` scripts), moved here with
  `git mv` (byte-identical, renamed). The empty `CoRR/templates/PDFs.download` and
  `PDFs.lock` marker files were removed.
- **protocol2bids corpus**: `datasets/pdf/<DATASET>/<site>/` and
  `datasets/nii/<DATASET>/<site>/` of
  <https://github.com/balbasty/protocol2bids> (commit `5b82670`), copied
  unchanged. Corpus files byte-identical to a brainspresso file were not
  copied again (all ABIDE-II scantables, BNI_1/EMC_1/ETH_1 exports, CoRR
  NKI_TRT and XHCUMS). Corpus sites `CoRR/IACAS` and `CoRR/UM` are stored as
  `IACAS_1` and `UM_1`.

ABIDE-I had no printouts in brainspresso; all come from the corpus.

### INDI projects

Single-site projects (site `""` in `brainspresso/datasets/indi/<project>/dataset.py`):
the files sit directly in `printouts/<DATASET>/` (no site folder), and the
templates in `brainspresso/datasets/indi/<project>/templates/`. Downloaded on 2026-11-27 from
`https://fcon_1000.projects.nitrc.org/indi/`, unchanged (only renamed where
said):

| dataset | file | source (under `indi/`) |
|---|---|---|
| SLIM | `anat.pdf`, `dti.pdf`, `rest.pdf` | `retro/swu_slim_content/swu_slim_scan_params_{anat,dti,rest}.pdf` (linked from `retro/southwestuni_qiu_index.html`) |
| SALD | - (by reference: `../SLIM/anat.pdf`, `../SLIM/rest.pdf`) | `retro/SALD/swu_slim_scan_params_{anat,rest}.pdf`, byte-identical to SLIM's (SHA-256 `95f50815...`, `945f2ca3...`) |
| MPI-LEMON | `1_LEMON_resting_130904.pdf`, `2_LEMON_anat_130904.pdf`, `3_LEMON_anat_MODIFIED_141008.pdf`, `4_MRI_Sequences_Table_LEMON.pdf` (was `4_MRI Sequences_Table_LEMON.pdf`) | `retro/MPI_LEMON/` |
| NKI-RS | `NKI_MPRAGE.pdf` | `enhanced/NKI_MPRAGE.pdf` |
| NKI-RS | `Checker_645.pdf`, `Checker_1400.pdf`, `Breath_1400.pdf` | `pro/eNKI_RS_TRT/` |
| NKI-RS | - (by reference: `../CoRR/NKI_TRT/{dti,rest_645,rest_1400,rest_2500}.pdf`) | `pro/eNKI_RS_TRT/{DIff_137,Rest_645,Rest_1400,Rest_CAP}.pdf`, byte-identical to the CoRR NKI_TRT printouts |

The NKI-RS protocol page (`enhanced/mri_protocol.html`) is HTML, which
protocol2bids does not read; it links one Siemens printout per scan, which
are used instead. Its real-time neurofeedback printouts (`enhanced/MPRAGE.pdf`,
`FieldMap.pdf`, `Mask.pdf`, `r-fMRI.pdf`, `RTfMRI.pdf`, `MSIT.pdf`,
`MoralDilemma.pdf`) are not stored: those scans are not in
`RocklandSample/RawDataBIDSLatest`. NKI-RS `T2w`, `FLAIR` and
`task-pCASL_bold` have no printout, hence no template. The `acq-*RR`
reruns are all bold scans (`task-*_acq-{645,1400,CAP}RR_bold`, 20 of the
24756 NIfTI files of the bucket, full listing of 2026-11-27): no template
applies to them, as their `acq` labels differ from the templates'
(`T1w.json` and `dwi.json` only apply to T1w and dwi files, none of which
has an `acq` entity). LEMON's
`4_MRI_Sequences_Table_LEMON.pdf` is a summary table that protocol2bids does
not read: it is kept for documentation, unused.

No printout was found for the other INDI projects: FCON1000 (only a summary
table of scan parameters), NATVIEW (only a PDF of download instructions),
SIMON, HBN and ACPI. They have no table here.

Per site:

### ABIDE1

| site | moved from brainspresso | copied from protocol2bids corpus | nii headers |
|---|---|---|---|
| CMU_a | - | `rest.pdf` | 2 |
| CMU_b | - | `anat.pdf`, `rest.pdf` | 2 |
| Caltech | - | `anat.pdf`, `rest.pdf` | 2 |
| KKI | - | `anat.pdf`, `rest.pdf` | 2 |
| Leuven_1 | - | `anat.pdf`, `rest.pdf` | 2 |
| Leuven_2 | - | `anat.pdf`, `rest.pdf` | 2 |
| MaxMun_a | - | `anat.pdf`, `rest.pdf` | 2 |
| MaxMun_b | - | `anat.pdf`, `rest.pdf` | 2 |
| MaxMun_c | - | `anat.pdf`, `rest.pdf` | 2 |
| MaxMun_d | - | `anat.pdf`, `rest.pdf` | 2 |
| NYU | - | `anat.pdf`, `rest.pdf` | 2 |
| OHSU | - | `anat.pdf`, `rest.pdf` | 2 |
| Olin | - | `anat.pdf`, `rest.pdf` | 2 |
| Pitt | - | `anat.pdf`, `rest.pdf` | 2 |
| SBL | - | `anat.pdf`, `rest.pdf` | 2 |
| SDSU | - | `anat.pdf`, `rest.pdf` | 2 |
| Stanford | - | `anat.pdf`, `rest.pdf` | 2 |
| Trinity | - | `anat.pdf`, `rest.pdf` | 2 |
| UCLA_1 | - | `anat.pdf`, `rest.pdf` | 3 |
| UCLA_2 | - | `anat.pdf`, `rest.pdf` | 3 |
| UM_1 | - | `anat.pdf`, `rest.pdf` | 2 |
| UM_2 | - | `anat.pdf`, `rest.pdf` | 2 |
| USM | - | `anat.pdf`, `rest.pdf` | 2 |
| Yale | - | `anat.pdf`, `rest.pdf` | 2 |

### ABIDE2

| site | moved from brainspresso | copied from protocol2bids corpus | nii headers |
|---|---|---|---|
| BNI_1 | `3DFLAIR.txt` (was `ABIDEII-BNI_1_3DFLAIR.txt`), `anat.txt` (was `ABIDEII-BNI_1_anat.txt`), `dti.txt` (was `ABIDEII-BNI_1_dti.txt`), `rest.txt` (was `ABIDEII-BNI_1_rest.txt`), `scantable.pdf` (was `ABIDEII-BNI_1_scantable.pdf`) | - | 4 |
| EMC_1 | `anat.pdf` (was `ABIDEII-EMC_1_anat.pdf`), `rest.pdf` (was `ABIDEII-EMC_1_rest.pdf`), `scantable.pdf` (was `ABIDEII-EMC_1_scantable.pdf`) | - | 2 |
| ETH_1 | `anat.txt` (was `ABIDEII-ETH_1_anat.txt`), `rest.txt` (was `ABIDEII-ETH_1_rest.txt`), `scantable.pdf` (was `ABIDEII-ETH_1_scantable.pdf`) | - | 2 |
| GU_1 | `scantable.pdf` (was `ABIDEII-GU_1_scantable.pdf`) | `T1w.pdf`, `bold.pdf` | 2 |
| IP_1 | `scantable.pdf` (was `ABIDEII-IP_1_scantable.pdf`) | `T1w.pdf`, `bold.pdf`, `dwi.pdf` | 3 |
| IU_1 | `scantable.pdf` (was `ABIDEII-IU_1_scantable.pdf`) | `T1w.pdf`, `bold.pdf` | 2 |
| KKI_1 | - | `acq-rc32chan_T1w.pdf`, `acq-rc8chan_T1w.pdf`, `bold.pdf` | 4 |
| KUL_3 | `scantable.pdf` (was `ABIDEII-KUL_3_scantable.pdf`) | `T1w.txt`, `bold.txt` | 2 |
| MIA_1 | - | `T1w.pdf`, `bold.pdf`, `sub-30230-30232_bold.pdf` | 0 |
| NYU_1 | `scantable.pdf` (was `ABIDEII-NYU_1_scantable.pdf`) | `T1w.pdf`, `bold.pdf`, `dwi.pdf`, `fieldmap.pdf` | 4 |
| NYU_2 | `scantable.pdf` (was `ABIDEII-NYU_2_scantable.pdf`) | `T1w.pdf`, `bold.pdf`, `dwi.pdf`, `fieldmap.pdf` | 4 |
| OHSU_1 | `scantable.pdf` (was `ABIDEII-OHSU_1_scantable.pdf`) | `T1w.pdf`, `all.pdf`, `bold.pdf` | 2 |
| OILH_2 | - | `scantable.pdf` | 0 |
| ONRC_2 | - | `T1w.pdf`, `bold.pdf` | 2 |
| SDSU_1 | `scantable.pdf` (was `ABIDEII-SDSU_1_scantable.pdf`) | `T1w.pdf`, `bold.pdf`, `dwi.pdf` | 3 |
| SU_2 | - | `T1w.pdf`, `bold.pdf` | 0 |
| TCD_1 | `scantable.pdf` (was `ABIDEII-TCD_1_scantable.pdf`) | `T1w.txt`, `bold.txt`, `dwi.txt` | 3 |
| UCD_1 | `scantable.pdf` (was `ABIDEII-UCD_1_scantable.pdf`) | `T1w.pdf`, `bold.pdf`, `sub-30004_bold.pdf` | 2 |
| UCLA_1 | `scantable.pdf` (was `ABIDEII-UCLA_1_scantable.pdf`) | `T1w.pdf`, `bold.pdf` | 3 |
| UCLA_Long | `scantable.pdf` (was `ABIDEII-UCLA_Long_scantable.pdf`) | `T1w.pdf`, `bold.pdf` | 3 |
| UPSM_Long | `scantable.pdf` (was `ABIDEII-UPSM_Long_scantable.pdf`) | `T1w.pdf`, `bold.pdf` | 2 |
| USM_1 | `scantable.pdf` (was `ABIDEII-USM_1_scantable.pdf`) | `T1w.pdf`, `bold.pdf` | 2 |

### CoRR

| site | moved from brainspresso | copied from protocol2bids corpus | nii headers |
|---|---|---|---|
| BMB_1 | `scantable.pdf` (was `BMB_1_scantable.pdf`) | `anat.pdf`, `rest.pdf` | 2 |
| BNU_1 | `scantable.pdf` (was `BNU_1_scantable.pdf`) | `anat.pdf`, `dti.pdf`, `rest.pdf` | 3 |
| BNU_2 | `scantable_retest.pdf` (was `BNU_2_Retest_scantable.pdf`), `scantable_test.pdf` (was `BNU_2_Test_scantable.pdf`) | `anat1.pdf`, `anat2.pdf`, `rest1.pdf`, `rest2.pdf` | 4 |
| BNU_3 | `scantable.pdf` (was `BNU_3_scantable.pdf`) | `anat.pdf`, `dti.pdf`, `rest.pdf` | 3 |
| DC_1 | `scantable.pdf` (was `DC_1_scantable.pdf`) | - | 0 |
| HNU_1 | `scantable.pdf` (was `HNU_1_scantable.pdf`) | - | 3 |
| IACAS_1 | `scantable.pdf` (was `IACAS_1_scantable.pdf`) | `anat.pdf`, `rest.pdf` | 2 |
| IBA_TRT | `scantable.pdf` (was `IBATRT_scantable.pdf`) | - | 2 |
| IPCAS_1 | `scantable.pdf` (was `IPCAS_1_scantable.pdf`) | - | 3 |
| IPCAS_2 | `scantable.pdf` (was `IPCAS_2_scantable.pdf`) | - | 3 |
| IPCAS_3 | `scantable.pdf` (was `IPCAS_3_scantable.pdf`) | - | 2 |
| IPCAS_4 | `scantable.pdf` (was `IPCAS_4_scantable.pdf`) | - | 2 |
| IPCAS_5 | `scantable.pdf` (was `IPCAS_5_scantable.pdf`) | `all.pdf` | 2 |
| IPCAS_6 | `scantable.pdf` (was `IPCAS_6_scantable.pdf`) | - | 2 |
| IPCAS_7 | `scantable.pdf` (was `IPCAS_7_scantable.pdf`) | - | 2 |
| IPCAS_8 | `scantable.pdf` (was `IPCAS_8_scantable.pdf`) | - | 3 |
| JHNU_1 | `scantable.pdf` (was `JHNU_1_scantable.pdf`) | - | 2 |
| LMU_1 | `scantable.pdf` (was `LMU_1_scantable.pdf`) | - | 2 |
| LMU_2 | `scantable.pdf` (was `LMU_2_scantable.pdf`) | - | 2 |
| LMU_3 | `scantable.pdf` (was `LMU_3_scantable.pdf`) | - | 2 |
| MPG_1 | `scantable.pdf` (was `MPG_1_scantable.pdf`) | - | 2 |
| MRN_1 | `scantable.pdf` (was `MRN_1_scantable.pdf`) | - | 3 |
| NKI_2 | `scantable.pdf` (was `NKI_2_scantable.pdf`) | - | 0 |
| NKI_TRT | `dti.pdf` (was `nki_DTI.pdf`), `rest_1400.pdf` (was `nki_rest_1400.pdf`), `rest_2500.pdf` (was `nki_rest_2500.pdf`), `rest_645.pdf` (was `nki_rest_645.pdf`) | - | 5 |
| NYU_1 | `scantable.pdf` (was `NYU_1_scantable.pdf`) | - | 2 |
| NYU_2 | `scantable.pdf` (was `NYU_2_scantable.pdf`) | `anat.pdf`, `dti.pdf`, `rest.pdf` | 2 |
| SWU_1 | `scantable.pdf` (was `SWU_1_scantable.pdf`) | - | 2 |
| SWU_2 | `scantable.pdf` (was `SWU_2_scantable.pdf`) | - | 2 |
| SWU_3 | `scantable.pdf` (was `SWU_3_scantable.pdf`) | - | 2 |
| SWU_4 | `scantable.pdf` (was `SWU_4_scantable.pdf`) | - | 3 |
| UM_1 | `scantable.pdf` (was `UM_1_scantable.pdf`) | `all.pdf` | 2 |
| UPSM_1 | `scantable.pdf` (was `UPSM_1_scantable.pdf`) | - | 2 |
| Utah_1 | `scantable.pdf` (was `Utah_1_scantable.pdf`) | `anat.pdf`, `fieldmap.pdf`, `rest.pdf` | 2 |
| Utah_2 | `scantable.pdf` (was `Utah_2_scantable.pdf`) | `anat.pdf`, `fieldmap.pdf`, `rest.pdf` | 3 |
| XHCUMS | `all.pdf` (was `beijing_li_all.pdf`) | - | 3 |

## Conversion tables

`<DATASET>/sidecars.toml` has one `[[sidecar]]` entry per sidecar template
(every acquisition sidecar under the dataset's `templates/` must be listed;
this is tested):

```toml
[[sidecar]]
output = "BNI/1/T1w.json"         # relative to the dataset's templates/
printout = "BNI_1/anat.txt"       # relative to printouts/<DATASET>/
select = "0"                      # optional: index or name glob (p2b list)
nii = "BNI_1/nii/T1w.nii"         # optional: NIfTI header
defaults = { MagneticFieldStrength = 3 }   # optional, used if missing
assigns = { Manufacturer = "Philips" }     # optional, overrides
note = "free text"                # optional

[[sidecar]]
output = "UCLA/2/T1w.json"
manual = true                     # no printout: kept as is

[[sidecar]]                       # INDI (printouts/SLIM/sidecars.toml)
output = "task-rest_bold.json"    # templates/slim/task-rest_bold.json
printout = "rest.pdf"
nii = "nii/bold.nii"
defaults = { TaskName = "rest" }
fill = true                       # fill-in template (see below)
```

`fill = true` marks a fill-in template: it never replaces a sidecar
hosted with the data (key-level merging, filling only the missing keys,
is the job of the placement hook; until it lands, see below).
It does not change the conversion. `sidecars.lock.json` records it as
`"fill": true`, and `make_sidecars.py` writes the list of fill-in
templates to `_fill.json` next to them (`{"fill": ["T1w.json", ...]}`,
checked by `--check`), which ships with the package (the lock does not):
this is the **fill marker** the engine reads. The fill-in hook and its
final convention (template location, flag or naming) belong to the
hosted-BIDS placement work (step 2b-iii); the names are single constants
of `make_sidecars.py` (`FILL_KEY`, `LOCK_FILL_KEY`, `FILL_INDEX`) and of
`indi/engine.py` (`FILL_INDEX`). Until that hook lands, the INDI engine
mirrors `indi/<project>/templates/*` at the dataset root, where BIDS
inheritance lets the hosted, lower-level sidecars win, and a bucket file
at the same path as a fill-in template is kept (the template is skipped,
with a warning; non-fill templates still override, as before).

`drop = [...]` removes keys from the converted sidecar (after `defaults`
and `assigns`; recorded in the lock). It is used for keys that would be
wrong when inherited: in MPI-LEMON `MP2RAGE.json`, `RepetitionTime`
(`bids.repair` moves it to `RepetitionTimePreparation` in
`*_MP2RAGE.json`, which the printout already gives), in
`acq-GEfmap_phasediff.json` `EchoTime` (the list of both echoes: a
phasediff has `EchoTime1`/`EchoTime2`; the hosted one has no `EchoTime`,
and `bids.repair` reads one as the second echo), in
`acq-mp2rage_{T1w,T1map}.json` `ImageType` (the printout's
ORIGINAL/PRIMARY; these images are derived by the scanner), and in NKI-RS
`T1w.json` `SliceEncodingDirection` (see "Voxel axes" below). No other
template has a key that `bids.repair` renames or removes. INDI outputs are named as the
local (remapped) BIDS files, without `sub`/`ses`/`run` entities, and bold
templates get `TaskName` (from the file name) as a `defaults`. Printouts
byte-identical to another dataset's are used by reference
(`printout = "../SLIM/anat.pdf"`), with the dataset's own NIfTI headers.

The richest printout available is used for each sidecar (vendor exports
over the ABIDE/CoRR "scantable" summaries). Choices worth knowing:

- ABIDE-I follows the owner's per-sample mapping of protocol2bids
  `datasets/convert_ABIDE1.sh`: sites with samples get one template folder
  per sample (`CMU_a/`, `CMU_b/`, `Leuven_1/`, `Leuven_2/`, `MaxMun_a/`..
  `MaxMun_d/`, `UCLA_1/`, `UCLA_2/`, `UM_1/`, `UM_2/`), declared in
  `BidsifierBase.SAMPLES`. The roast parses the site name of the data in
  any source spelling (S3 `RawData/MaxMun_a/`, NITRC `Leuven_1.tgz`, XNAT
  `UCLA1`/`Leuven-1`, phenotypic `MAX_MUN`) and uses
  `templates/<Site>_<sample>/<suffix>.json`, else
  `templates/<Site>/<suffix>.json` (`CMU/T1w.json` is kept for `CMU_a`,
  which has no anatomical printout). When the data names no sample (XNAT
  `CMU_<id>`, `MaxMun`), it uses `templates/<Site>/<suffix>.json` if it
  exists, a `common` entry holding only the keys the samples share
  (`CMU/bold.json`, `MaxMun/bold.json`; a warning is logged; the bold
  `RepetitionTime` that CMU's samples do not share is then read from the
  NIfTI header, or the sidecar is an error), else the
  first sample (only where all samples share one protocol, which the
  tests check). Sites without samples accept only a tarball part as
  suffix (`NYU_a`). An unknown site, sample or suffix, or a missing
  template, is an error naming both, counted by the roast. `UCLA_{1,2}/acq-hires_T1w.json` is
  the high-resolution anatomical (`anat_hires`, protocol 1 of `anat.pdf`),
  not read by the roast yet.
- ABIDE-II follows `convert_ABIDE2.sh`, except that the richer vendor
  exports (`*.txt`) are kept over the scantable for BNI_1, ETH_1, KUL_3 and
  TCD_1 (more keys: parallel imaging, readout, slice timing). New templates
  `KKI/1/acq-rc{8,32}chan_{T1w,bold}.json`, `NYU/{1,2}/fieldmap.json` and
  `ONRC/2/{T1w,bold}.json` are not read by the roast yet. `UCLA/2` and
  `UPSM/1` have no printout and are `manual`.
- CoRR follows `convert_CoRR.sh` for BMB_1, BNU_1-3, IACAS, IPCAS_5,
  NKI_TRT, NYU_2, UM, Utah_1-2 and XHCUMS (scantables elsewhere). Several
  protocols of one sample use BIDS-like names: `BNU/2/ses-{1,2}_{T1w,bold}.json`
  (test/retest), `NKI/TRT/acq-{645,1400,2500}_bold.json` (+ `dwi.json`),
  `Utah/2/fieldmap.json`, `XHCUMS/1/{T1w,dwi,bold}.json`. `NKI/2` keeps the
  `NKI_2/scantable.pdf` table. CoRR has no roast yet.
- INDI (all fill-in): SLIM `T1w`, `dwi`, `task-rest_bold` (the latter
  replaces the hand-written `{TaskName, RepetitionTime}`; the bucket hosts
  no SLIM sidecar); SALD `T1w`, `task-rest_bold` (SLIM's printouts);
  MPI-LEMON, after `LEMON.remap`: `task-rest_acq-AP_bold`,
  `acq-GEfmap_phasediff`, `acq-SEfmapBOLD{pre,post}_dir-{AP,PA}_epi`
  (`cmrr_mbep2d_se` is AP, the polarity of the AP bold, `_invpol` PA),
  `MP2RAGE` (both inversions), `acq-mp2rage_{T1w,T1map}`, `T2w`,
  `acq-lowres_FLAIR` (2D, first anatomical printout), `acq-highres_FLAIR`
  (3D, modified printout), `T2starw` (`as_gre_TE17ms`, both parts; the
  bucket's GRE has TE = 17.3 ms and only subjects with the 3D FLAIR have
  it), `dwi`, `acq-SEfmapDWI_dir-{AP,PA}_epi`; NKI-RS `T1w`, `dwi`,
  `task-rest_acq-{645,1400,CAP}_bold`, `task-CHECKERBOARD_acq-{645,1400}_bold`,
  `task-BREATHHOLD_acq-1400_bold`. LEMON's printouts are summary exports
  (10 to 19 keys); SLIM's and NKI-RS's are full Siemens B17 printouts (42
  to 48 keys).

## NIfTI headers

Every converted entry has a `nii` header (protocol2bids needs it to map
phase-encoding/slice directions to voxel axes), except CoRR `DC/1/*` and
`NKI/2/*`, which have no matching scan on S3 (`CORR/RawData/` has no `DC_1`
or `NKI_2` folder). Headers come from the protocol2bids corpus
(`datasets/nii/`), or were fetched from the public `fcp-indi` bucket with
`scripts/fetch_nii_headers.py` (first 64 KiB, header kept, data stripped):

| header | source key (under `s3://fcp-indi/data/Projects/`) |
|---|---|
| `ABIDE2/BNI_1/nii/FLAIR.nii` | `ABIDE2/RawData/ABIDEII-BNI_1/sub-29015/ses-1/anat/sub-29015_ses-1_run-1_FLAIR.nii.gz` |
| `ABIDE2/GU_1/nii/bold.nii` | `ABIDE2/RawData/ABIDEII-GU_1/sub-28742/ses-1/func/sub-28742_ses-1_task-rest_run-1_bold.nii.gz` |
| `ABIDE2/SDSU_1/nii/dwi.nii` | `ABIDE2/RawData/ABIDEII-SDSU_1/sub-28853/ses-1/dwi/sub-28853_ses-1_run-1_dwi.nii.gz` |
| `SLIM/nii/T1w.nii` | `INDI/SLIM/RawData/sub-25629/ses-1/anat/sub-25629_ses-1_T1w.nii.gz` |
| `SLIM/nii/dwi.nii` | `INDI/SLIM/RawData/sub-25629/ses-1/dwi/sub-25629_ses-1_dwi.nii.gz` |
| `SLIM/nii/bold.nii` | `INDI/SLIM/RawData/sub-25629/ses-1/func/sub-25629_ses-1_task-rest_bold.nii.gz` |
| `SALD/nii/T1w.nii` | `INDI/SALD/RawData_BIDS/sub-031274/anat/sub-031274_T1w.nii.gz` |
| `SALD/nii/bold.nii` | `INDI/SALD/RawData_BIDS/sub-031274/func/sub-031274_task-rest_bold.nii.gz` |
| `MPI-LEMON/nii/bold.nii` | `INDI/MPI-LEMON/MRI_MPILMBB_LEMON/MRI_Raw/sub-032301/ses-01/func/sub-032301_ses-01_task-rest_acq-AP_run-01_bold.nii.gz` |
| `MPI-LEMON/nii/phasediff.nii` | `INDI/MPI-LEMON/MRI_MPILMBB_LEMON/MRI_Raw/sub-032301/ses-01/fmap/sub-032301_ses-01_acq-GEfmap_run-01_phasediff.nii.gz` |
| `MPI-LEMON/nii/SEfmapBOLDpre_AP.nii` | `INDI/MPI-LEMON/MRI_MPILMBB_LEMON/MRI_Raw/sub-032301/ses-01/fmap/sub-032301_ses-01_acq-SEfmapBOLDpre_dir-AP_epi.nii.gz` |
| `MPI-LEMON/nii/SEfmapBOLDpre_PA.nii` | `INDI/MPI-LEMON/MRI_MPILMBB_LEMON/MRI_Raw/sub-032301/ses-01/fmap/sub-032301_ses-01_acq-SEfmapBOLDpre_dir-PA_epi.nii.gz` |
| `MPI-LEMON/nii/SEfmapBOLDpost_AP.nii` | `INDI/MPI-LEMON/MRI_MPILMBB_LEMON/MRI_Raw/sub-032301/ses-01/fmap/sub-032301_ses-01_acq-SEfmapBOLDpost_dir-AP_epi.nii.gz` |
| `MPI-LEMON/nii/SEfmapBOLDpost_PA.nii` | `INDI/MPI-LEMON/MRI_MPILMBB_LEMON/MRI_Raw/sub-032301/ses-01/fmap/sub-032301_ses-01_acq-SEfmapBOLDpost_dir-PA_epi.nii.gz` |
| `MPI-LEMON/nii/SEfmapDWI_AP.nii` | `INDI/MPI-LEMON/MRI_MPILMBB_LEMON/MRI_Raw/sub-032301/ses-01/fmap/sub-032301_ses-01_acq-SEfmapDWI_dir-AP_epi.nii.gz` |
| `MPI-LEMON/nii/SEfmapDWI_PA.nii` | `INDI/MPI-LEMON/MRI_MPILMBB_LEMON/MRI_Raw/sub-032301/ses-01/fmap/sub-032301_ses-01_acq-SEfmapDWI_dir-PA_epi.nii.gz` |
| `MPI-LEMON/nii/inv-1_MP2RAGE.nii` | `INDI/MPI-LEMON/MRI_MPILMBB_LEMON/MRI_Raw/sub-032301/ses-01/anat/sub-032301_ses-01_inv-1_mp2rage.nii.gz` |
| `MPI-LEMON/nii/mp2rage_T1w.nii` | `INDI/MPI-LEMON/MRI_MPILMBB_LEMON/MRI_Raw/sub-032301/ses-01/anat/sub-032301_ses-01_acq-mp2rage_T1w.nii.gz` |
| `MPI-LEMON/nii/mp2rage_T1map.nii` | `INDI/MPI-LEMON/MRI_MPILMBB_LEMON/MRI_Raw/sub-032301/ses-01/anat/sub-032301_ses-01_acq-mp2rage_T1map.nii.gz` |
| `MPI-LEMON/nii/T2w.nii` | `INDI/MPI-LEMON/MRI_MPILMBB_LEMON/MRI_Raw/sub-032301/ses-01/anat/sub-032301_ses-01_T2w.nii.gz` |
| `MPI-LEMON/nii/lowres_FLAIR.nii` | `INDI/MPI-LEMON/MRI_MPILMBB_LEMON/MRI_Raw/sub-032301/ses-01/anat/sub-032301_ses-01_acq-lowres_FLAIR.nii.gz` |
| `MPI-LEMON/nii/highres_FLAIR.nii` | `INDI/MPI-LEMON/MRI_MPILMBB_LEMON/MRI_Raw/sub-032328/ses-01/anat/sub-032328_ses-01_acq-highres_FLAIR.nii.gz` |
| `MPI-LEMON/nii/mag_GRE.nii` | `INDI/MPI-LEMON/MRI_MPILMBB_LEMON/MRI_Raw/sub-032328/ses-01/anat/sub-032328_ses-01_acq-mag_GRE.nii.gz` |
| `MPI-LEMON/nii/dwi.nii` | `INDI/MPI-LEMON/MRI_MPILMBB_LEMON/MRI_Raw/sub-032301/ses-01/dwi/sub-032301_ses-01_dwi.nii.gz` |
| `NKI-RS/nii/T1w.nii` | `RocklandSample/RawDataBIDSLatest/sub-A00008326/ses-BAS1/anat/sub-A00008326_ses-BAS1_T1w.nii.gz` |
| `NKI-RS/nii/dwi.nii` | `RocklandSample/RawDataBIDSLatest/sub-A00008326/ses-BAS1/dwi/sub-A00008326_ses-BAS1_dwi.nii.gz` |
| `NKI-RS/nii/rest_645.nii` | `RocklandSample/RawDataBIDSLatest/sub-A00008326/ses-BAS1/func/sub-A00008326_ses-BAS1_task-rest_acq-645_bold.nii.gz` |
| `NKI-RS/nii/rest_1400.nii` | `RocklandSample/RawDataBIDSLatest/sub-A00008326/ses-BAS1/func/sub-A00008326_ses-BAS1_task-rest_acq-1400_bold.nii.gz` |
| `NKI-RS/nii/rest_CAP.nii` | `RocklandSample/RawDataBIDSLatest/sub-A00008326/ses-BAS1/func/sub-A00008326_ses-BAS1_task-rest_acq-CAP_bold.nii.gz` |
| `NKI-RS/nii/checker_645.nii` | `RocklandSample/RawDataBIDSLatest/sub-A00008326/ses-BAS1/func/sub-A00008326_ses-BAS1_task-CHECKERBOARD_acq-645_bold.nii.gz` |
| `NKI-RS/nii/checker_1400.nii` | `RocklandSample/RawDataBIDSLatest/sub-A00008326/ses-BAS1/func/sub-A00008326_ses-BAS1_task-CHECKERBOARD_acq-1400_bold.nii.gz` |
| `NKI-RS/nii/breathhold_1400.nii` | `RocklandSample/RawDataBIDSLatest/sub-A00008326/ses-BAS1/func/sub-A00008326_ses-BAS1_task-BREATHHOLD_acq-1400_bold.nii.gz` |

## Voxel axes of the INDI templates

`PhaseEncodingDirection`, `SliceEncodingDirection` and the order of
`SliceTiming` refer to voxel axes, mapped by protocol2bids from ONE NIfTI
header, while a top-level template applies to every subject and session.
For each INDI template with such keys, the headers of 10 files of the
bucket (evenly spaced in the sorted listing, so across subjects and
sessions; first 64 KiB, `fetch_nii_headers.fetch_header`) were compared
with the reference header (qform/sform axis codes, nibabel), on
2026-11-27:

| template | files in the bucket | sessions sampled | axis codes | result |
|---|---|---|---|---|
| SLIM `T1w`, `dwi`, `task-rest_bold` | 1048, 1029, 1044 | ses-1, 2, 3 | LAS (q and s), 10/10 | kept |
| SALD `T1w` | 494 | - | RAS (sform; no qform), 10/10 | kept |
| SALD `task-rest_bold` | 493 | - | LAS, 10/10 | kept |
| NKI-RS `dwi`, `task-rest_acq-{645,1400,CAP}_bold`, `task-CHECKERBOARD_acq-{645,1400}_bold`, `task-BREATHHOLD_acq-1400_bold` | 2352 to 2487 each | 3 to 5 of BAS1-3, FLU1-3, TRT | LAS, 10/10 each | kept |
| NKI-RS `T1w` | 2455 | BAS1, BAS2, FLU1, FLU2, TRT | LAS 9/10, RAS 1/10 (`sub-A00089407/ses-FLU1`) | `SliceEncodingDirection` dropped |

MPI-LEMON templates have none of these keys (summary printouts). A sample
does not prove that every file agrees; a disagreeing hosted sidecar
still wins (fill-in).

## Regenerating

```sh
pip install -r requirements-maint.txt   # protocol2bids (maintainers only)
python scripts/make_sidecars.py ABIDE2               # write sidecars
python scripts/make_sidecars.py ABIDE2 --only 'BNI/*'
python scripts/make_sidecars.py ABIDE2 --check       # CI: non-zero if stale
```

For each entry the script reads the printout with protocol2bids
(`read_protocols`, `protocol_to_bids(..., extra=False)`, i.e. BIDS keys
only), fills missing keys from `fill_from` (another printout of the same
acquisition, e.g. the site scan table, as `p2b --fill-from`; used where
protocol2bids' `datasets/convert_*.sh` use it), applies `defaults` and
`assigns`, and writes the sidecar (4-space indent, protocol2bids key order,
trailing newline). It never adds
non-BIDS keys to sidecars; provenance (printout SHA-256, selector, NIfTI
header SHA-256, and per sidecar the protocol2bids version and commit;
the top-level `protocol2bids` is "mixed" if they differ) goes to
`sidecars.lock.json`. `report.md` lists, per sidecar, its protocol2bids
version and baseline and the keys added, removed and changed compared to
git `HEAD` (or `--baseline REF`). The committed reports use
`--baseline aa3e2f1` (the hand-parsed templates). An entry with
`common = [...]` (no printout) holds the keys whose values agree across
the listed sidecars.

Do not hand-fix values in the generated sidecars: fix protocol2bids (or, for
genuinely missing information, add `defaults`/`assigns`) and re-run.

## Superseded code

`brainspresso/utils/prot/` and the `abide2_parse_scan_param.py` /
`corr_parse_scan_param.py` scripts that used to live in the templates
folders were superseded by this workflow and have been removed.
