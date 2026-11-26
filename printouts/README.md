# Protocol printouts

This folder holds the MRI protocol printouts (scanner exports, scan
parameter tables) that brainspresso uses to build the BIDS sidecar
templates of datasets whose providers do not distribute sidecars:

| folder    | dataset  | templates                                |
| --------- | -------- | ---------------------------------------- |
| `ABIDE1/` | ABIDE-I  | `brainspresso/datasets/ABIDE/I/templates` |
| `ABIDE2/` | ABIDE-II | `brainspresso/datasets/ABIDE/II/templates` |
| `CoRR/`   | CoRR     | `brainspresso/datasets/CoRR/templates`    |

It lives **outside** the `brainspresso` package and is never shipped in
the wheel/sdist. The printouts are converted once, by a maintainer, with
[protocol2bids](https://github.com/balbasty/protocol2bids); the generated
sidecars are committed into the templates. protocol2bids is **not** a
runtime dependency.

## Layout

```
printouts/<DATASET>/
    <SITE>_<SAMPLE>/           # e.g. ABIDE2/BNI_1, CoRR/BMB_1, ABIDE1/UCLA_1
        *.pdf, *.txt           # printouts
        nii/*.nii              # NIfTI headers only (data stripped), used to
                               # map anatomical directions to voxel axes
    sidecars.toml              # conversion table (printout -> sidecar)
    sidecars.lock.json         # provenance of the committed sidecars
    known_issues.md            # classified differences (copied
                               # at the top of report.md)
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
  `brainspresso/datasets/{ABIDE/II,CoRR}/templates/PDFs/` (downloaded by the
  old `*_parse_scan_param.py` scripts), moved here with `git mv`
  (byte-identical, renamed). The empty `CoRR/templates/PDFs.download` and
  `PDFs.lock` marker files were removed.
- **protocol2bids corpus**: `datasets/pdf/<DATASET>/<site>/` and
  `datasets/nii/<DATASET>/<site>/` of
  <https://github.com/balbasty/protocol2bids> (commit `5b82670`), copied
  unchanged. Corpus files byte-identical to a brainspresso file were not
  copied again (all ABIDE-II scantables, BNI_1/EMC_1/ETH_1 exports, CoRR
  NKI_TRT and XHCUMS). Corpus sites `CoRR/IACAS` and `CoRR/UM` are stored as
  `IACAS_1` and `UM_1`.

ABIDE-I had no printouts in brainspresso; all come from the corpus.

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
```

The richest printout available is used for each sidecar (vendor exports
over the ABIDE/CoRR "scantable" summaries). Choices worth knowing:

- ABIDE-I follows the owner's per-sample mapping of protocol2bids
  `datasets/convert_ABIDE1.sh`: sites with samples get one template folder
  per sample (`CMU_a/`, `CMU_b/`, `Leuven_1/`, `Leuven_2/`, `MaxMun_a/`..
  `MaxMun_d/`, `UCLA_1/`, `UCLA_2/`, `UM_1/`, `UM_2/`). The roast uses
  `templates/<Site>_<sample>/<suffix>.json` when the site name of the data
  (S3 `RawData/<Site>_<sample>/`, NITRC `<Site>_<sample>.tgz`) carries a
  sample, else `templates/<Site>/<suffix>.json` (`CMU/T1w.json` is kept as
  the fallback of `CMU_a`, which has no anatomical printout; NITRC parts
  such as `NYU_a` fall back to `NYU/`). `UCLA_{1,2}/acq-hires_T1w.json` is
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

## Regenerating

```sh
pip install -e .[maint]       # installs protocol2bids (maintainers only)
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
header SHA-256, protocol2bids version and commit) goes to
`sidecars.lock.json`. `report.md` lists, per sidecar, the keys added,
removed and changed compared to git `HEAD` (or `--baseline REF`),
preceded by `known_issues.md`. `known_issues.md` is generated (do not edit):
it classifies every difference into renames (`ManufacturerModelName` ->
`ManufacturersModelName`: the old key was misspelt), (a) old-template
errors corrected by protocol2bids (invalid values: non-`i`/`j`/`k`
directions, text `SliceTiming`, `SliceTiming` in anatomical sidecars,
implausible `EffectiveEchoSpacing`, `RepetitionTimeExcitation` in bold
sidecars...), accepted protocol2bids conventions (documented in
protocol2bids `notes/conversion_rules.md`: MPRAGE `RepetitionTime` is the
inversion TR, EPI TR in `RepetitionTime`, verified template errors; one
reason each, table `ACCEPTED` in the script), (b) suspected protocol2bids
issues, and non-BIDS keys of the old templates. The committed reports use
`--baseline aa3e2f1` (the hand-parsed templates).

Do not hand-fix values in the generated sidecars: fix protocol2bids (or, for
genuinely missing information, add `defaults`/`assigns`) and re-run.

## Superseded code

`brainspresso/utils/prot/` and the `abide2_parse_scan_param.py` /
`corr_parse_scan_param.py` scripts in the templates folders are superseded
by this workflow (they still refer to the old `templates/PDFs/` folders).
They will be removed in a later change.
