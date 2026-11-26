# CoRR sidecar regeneration report

## Suspected protocol2bids problems

Collected automatically by comparing the regenerated sidecars with the
previous (hand-parsed) templates. Values were NOT hand-fixed: fix
protocol2bids (or add `defaults`/`assigns` in `sidecars.toml`) and re-run.

### `ManufacturerModelName` dropped (77)

- `BMB/1/T1w.json` <- `BMB_1/anat.pdf`: was "TrioTim"
- `BMB/1/bold.json` <- `BMB_1/rest.pdf`: was "TrioTim"
- `BNU/1/T1w.json` <- `BNU_1/anat.pdf`: was "TrioTim"
- `BNU/1/bold.json` <- `BNU_1/rest.pdf`: was "TrioTim"
- `BNU/1/dwi.json` <- `BNU_1/dti.pdf`: was "TrioTim"
- `BNU/2/T1w.json` <- `BNU_2/anat1.pdf`: was "TrioTim"
- `BNU/2/bold.json` <- `BNU_2/rest1.pdf`: was "TrioTim"
- `BNU/3/T1w.json` <- `BNU_3/anat.pdf`: was "TrioTim"
- `BNU/3/bold.json` <- `BNU_3/rest.pdf`: was "TrioTim"
- `BNU/3/dwi.json` <- `BNU_3/dti.pdf`: was "TrioTim"
- `DC/1/T1w.json` <- `DC_1/scantable.pdf` (select `*Anatomical`): was "N/A"
- `DC/1/bold.json` <- `DC_1/scantable.pdf` (select `*Rest`): was "N/A"
- `HNU/1/T1w.json` <- `HNU_1/scantable.pdf` (select `*Anatomical`): was "Discovery MR750"
- `HNU/1/bold.json` <- `HNU_1/scantable.pdf` (select `*Rest`): was "Discovery MR750"
- `HNU/1/dwi.json` <- `HNU_1/scantable.pdf` (select `*DTI`): was "Discovery MR750"
- `IACAS/1/T1w.json` <- `IACAS_1/anat.pdf`: was "Signa HDx"
- `IACAS/1/bold.json` <- `IACAS_1/rest.pdf`: was "Signa HDx"
- `IBA/TRT/T1w.json` <- `IBA_TRT/scantable.pdf` (select `*Anatomical`): was "TrioTim"
- `IBA/TRT/bold.json` <- `IBA_TRT/scantable.pdf` (select `*Rest`): was "TrioTim"
- `IPCAS/1/T1w.json` <- `IPCAS_1/scantable.pdf` (select `*Anatomical`): was "TrioTim"
- `IPCAS/1/bold.json` <- `IPCAS_1/scantable.pdf` (select `*Rest`): was "TrioTim"
- `IPCAS/1/dwi.json` <- `IPCAS_1/scantable.pdf` (select `*DTI`): was "TrioTim"
- `IPCAS/2/T1w.json` <- `IPCAS_2/scantable.pdf` (select `*Anatomical`): was "TrioTim"
- `IPCAS/2/bold.json` <- `IPCAS_2/scantable.pdf` (select `*Rest`): was "TrioTim"
- `IPCAS/2/dwi.json` <- `IPCAS_2/scantable.pdf` (select `*DTI`): was "TrioTim"
- `IPCAS/3/T1w.json` <- `IPCAS_3/scantable.pdf` (select `*Anatomical`): was "TrioTim"
- `IPCAS/3/bold.json` <- `IPCAS_3/scantable.pdf` (select `*Rest`): was "TrioTim"
- `IPCAS/4/T1w.json` <- `IPCAS_4/scantable.pdf` (select `*Anatomical`): was "Discovery"
- `IPCAS/4/bold.json` <- `IPCAS_4/scantable.pdf` (select `*Rest`): was "Discovery MR750"
- `IPCAS/5/T1w.json` <- `IPCAS_5/all.pdf` (select `3`): was "TrioTim"
- `IPCAS/5/bold.json` <- `IPCAS_5/all.pdf` (select `1`): was "TrioTim"
- `IPCAS/6/T1w.json` <- `IPCAS_6/scantable.pdf` (select `*Anatomical`): was "TrioTim"
- `IPCAS/6/bold.json` <- `IPCAS_6/scantable.pdf` (select `*Rest`): was "TrioTim"
- `IPCAS/7/T1w.json` <- `IPCAS_7/scantable.pdf` (select `*Anatomical`): was "TrioTim"
- `IPCAS/7/bold.json` <- `IPCAS_7/scantable.pdf` (select `*Rest`): was "TrioTim"
- `IPCAS/8/T1w.json` <- `IPCAS_8/scantable.pdf` (select `*Anatomical`): was "TrioTim"
- `IPCAS/8/bold.json` <- `IPCAS_8/scantable.pdf` (select `*Rest`): was "TrioTim"
- `IPCAS/8/dwi.json` <- `IPCAS_8/scantable.pdf` (select `*DTI`): was "TrioTim"
- `JHNU/1/T1w.json` <- `JHNU_1/scantable.pdf` (select `*Anatomical`): was "TrioTim"
- `JHNU/1/bold.json` <- `JHNU_1/scantable.pdf` (select `*Rest`): was "TrioTim"
- `LMU/1/T1w.json` <- `LMU_1/scantable.pdf` (select `*Anatomical`): was "Achieva"
- `LMU/1/bold.json` <- `LMU_1/scantable.pdf` (select `*Rest`): was "Achieva"
- `LMU/2/T1w.json` <- `LMU_2/scantable.pdf` (select `*Anatomical`): was "Verio"
- `LMU/2/bold.json` <- `LMU_2/scantable.pdf` (select `*Rest`): was "Verio"
- `LMU/3/T1w.json` <- `LMU_3/scantable.pdf` (select `*Anatomical`): was "TrioTim"
- `LMU/3/bold.json` <- `LMU_3/scantable.pdf` (select `*Rest`): was "TrioTim"
- `MPG/1/T1w.json` <- `MPG_1/scantable.pdf` (select `*Anatomical`): was "Magnetom"
- `MPG/1/bold.json` <- `MPG_1/scantable.pdf` (select `*Rest`): was "Magentom"
- `MRN/1/T1w.json` <- `MRN_1/scantable.pdf` (select `*Anatomical`): was "TrioTim"
- `MRN/1/bold.json` <- `MRN_1/scantable.pdf` (select `*Rest`): was "TrioTim"
- `MRN/1/dwi.json` <- `MRN_1/scantable.pdf` (select `*DTI`): was "TrioTim"
- `NKI/2/T1w.json` <- `NKI_2/scantable.pdf` (select `*Anatomical`): was "TrioTim"
- `NKI/2/bold_TE=1400.json` <- `NKI_2/scantable.pdf` (select `*Rest 1400`): was "TrioTim"
- `NKI/2/bold_TE=2500.json` <- `NKI_2/scantable.pdf` (select `*Rest 2500`): was "TrioTim"
- `NKI/2/bold_TE=645.json` <- `NKI_2/scantable.pdf` (select `*Rest 645`): was "TrioTim"
- `NKI/2/dwi.json` <- `NKI_2/scantable.pdf` (select `*DTI`): was "TrioTim"
- `NYU/1/T1w.json` <- `NYU_1/scantable.pdf` (select `*Anatomical`): was "Allegra"
- `NYU/1/bold.json` <- `NYU_1/scantable.pdf` (select `*Rest`): was "Allegra"
- `NYU/2/T1w.json` <- `NYU_2/anat.pdf`: was "Allegra"
- `NYU/2/bold.json` <- `NYU_2/rest.pdf`: was "Allegra"
- `SWU/1/T1w.json` <- `SWU_1/scantable.pdf` (select `*Anatomical`): was "TrioTim"
- `SWU/1/bold.json` <- `SWU_1/scantable.pdf` (select `*Rest`): was "TrioTim"
- `SWU/2/T1w.json` <- `SWU_2/scantable.pdf` (select `*Anatomical`): was "TrioTim"
- `SWU/2/bold.json` <- `SWU_2/scantable.pdf` (select `*Rest`): was "TrioTim"
- `SWU/3/T1w.json` <- `SWU_3/scantable.pdf` (select `*Anatomical`): was "TrioTim"
- `SWU/3/bold.json` <- `SWU_3/scantable.pdf` (select `*Rest`): was "TrioTim"
- `SWU/4/T1w.json` <- `SWU_4/scantable.pdf` (select `*Anatomical`): was "TrioTim"
- `SWU/4/bold.json` <- `SWU_4/scantable.pdf` (select `*Rest`): was "TrioTim"
- `SWU/4/dwi.json` <- `SWU_4/scantable.pdf` (select `*DTI`): was "TrioTim"
- `UM/1/T1w.json` <- `UM_1/all.pdf` (select `1`): was "TrioTim"
- `UM/1/bold.json` <- `UM_1/all.pdf` (select `0`): was "TrioTim"
- `UPSM/1/T1w.json` <- `UPSM_1/scantable.pdf` (select `*Anatomical`): was "TrioTim"
- `UPSM/1/bold.json` <- `UPSM_1/scantable.pdf` (select `*Rest`): was "TrioTim"
- `Utah/1/T1w.json` <- `Utah_1/anat.pdf`: was "TrioTim"
- `Utah/1/bold.json` <- `Utah_1/rest.pdf`: was "TrioTim"
- `Utah/2/T1w.json` <- `Utah_2/anat.pdf`: was "TrioTim"
- `Utah/2/bold.json` <- `Utah_2/rest.pdf`: was "TrioTim"

### `PhaseEncodingDirection` dropped (61)

- `BMB/1/T1w.json` <- `BMB_1/anat.pdf`: was "AP"
- `BNU/1/T1w.json` <- `BNU_1/anat.pdf`: was "AP"
- `BNU/2/T1w.json` <- `BNU_2/anat1.pdf`: was "AP"
- `BNU/3/T1w.json` <- `BNU_3/anat.pdf`: was "AP"
- `DC/1/T1w.json` <- `DC_1/scantable.pdf` (select `*Anatomical`): was "RL"
- `DC/1/bold.json` <- `DC_1/scantable.pdf` (select `*Rest`): was "AP"
- `HNU/1/T1w.json` <- `HNU_1/scantable.pdf` (select `*Anatomical`): was "AP"
- `HNU/1/bold.json` <- `HNU_1/scantable.pdf` (select `*Rest`): was "AP"
- `HNU/1/dwi.json` <- `HNU_1/scantable.pdf` (select `*DTI`): was "RL"
- `IACAS/1/T1w.json` <- `IACAS_1/anat.pdf`: was "RL"
- `IACAS/1/bold.json` <- `IACAS_1/rest.pdf`: was "RL"
- `IBA/TRT/T1w.json` <- `IBA_TRT/scantable.pdf` (select `*Anatomical`): was "AP"
- `IBA/TRT/bold.json` <- `IBA_TRT/scantable.pdf` (select `*Rest`): was "AP"
- `IPCAS/1/T1w.json` <- `IPCAS_1/scantable.pdf` (select `*Anatomical`): was "AP"
- `IPCAS/1/bold.json` <- `IPCAS_1/scantable.pdf` (select `*Rest`): was "AP"
- `IPCAS/2/T1w.json` <- `IPCAS_2/scantable.pdf` (select `*Anatomical`): was "AP"
- `IPCAS/2/bold.json` <- `IPCAS_2/scantable.pdf` (select `*Rest`): was "AP"
- `IPCAS/3/T1w.json` <- `IPCAS_3/scantable.pdf` (select `*Anatomical`): was "AP"
- `IPCAS/3/bold.json` <- `IPCAS_3/scantable.pdf` (select `*Rest`): was "AP"
- `IPCAS/4/T1w.json` <- `IPCAS_4/scantable.pdf` (select `*Anatomical`): was "AP"
- `IPCAS/4/bold.json` <- `IPCAS_4/scantable.pdf` (select `*Rest`): was "AP"
- `IPCAS/5/T1w.json` <- `IPCAS_5/all.pdf` (select `3`): was "AP"
- `IPCAS/6/T1w.json` <- `IPCAS_6/scantable.pdf` (select `*Anatomical`): was "AP"
- `IPCAS/6/bold.json` <- `IPCAS_6/scantable.pdf` (select `*Rest`): was "AP"
- `IPCAS/7/T1w.json` <- `IPCAS_7/scantable.pdf` (select `*Anatomical`): was "AP"
- `IPCAS/7/bold.json` <- `IPCAS_7/scantable.pdf` (select `*Rest`): was "AP"
- `IPCAS/8/T1w.json` <- `IPCAS_8/scantable.pdf` (select `*Anatomical`): was "AP"
- `IPCAS/8/bold.json` <- `IPCAS_8/scantable.pdf` (select `*Rest`): was "AP"
- `IPCAS/8/dwi.json` <- `IPCAS_8/scantable.pdf` (select `*DTI`): was "AP"
- `JHNU/1/T1w.json` <- `JHNU_1/scantable.pdf` (select `*Anatomical`): was "AP"
- `JHNU/1/bold.json` <- `JHNU_1/scantable.pdf` (select `*Rest`): was "AP"
- `LMU/1/T1w.json` <- `LMU_1/scantable.pdf` (select `*Anatomical`): was "RL"
- `LMU/1/bold.json` <- `LMU_1/scantable.pdf` (select `*Rest`): was "AP"
- `LMU/2/T1w.json` <- `LMU_2/scantable.pdf` (select `*Anatomical`): was "AP"
- `LMU/2/bold.json` <- `LMU_2/scantable.pdf` (select `*Rest`): was "AP"
- `LMU/3/T1w.json` <- `LMU_3/scantable.pdf` (select `*Anatomical`): was "AP"
- `LMU/3/bold.json` <- `LMU_3/scantable.pdf` (select `*Rest`): was "AP"
- `MPG/1/T1w.json` <- `MPG_1/scantable.pdf` (select `*Anatomical`): was "AP"
- `MPG/1/bold.json` <- `MPG_1/scantable.pdf` (select `*Rest`): was "AP"
- `MRN/1/T1w.json` <- `MRN_1/scantable.pdf` (select `*Anatomical`): was "AP"
- `MRN/1/bold.json` <- `MRN_1/scantable.pdf` (select `*Rest`): was "AP"
- `MRN/1/dwi.json` <- `MRN_1/scantable.pdf` (select `*DTI`): was "AP"
- `NKI/2/T1w.json` <- `NKI_2/scantable.pdf` (select `*Anatomical`): was "AP"
- `NKI/2/bold_TE=1400.json` <- `NKI_2/scantable.pdf` (select `*Rest 1400`): was "AP"
- `NKI/2/bold_TE=2500.json` <- `NKI_2/scantable.pdf` (select `*Rest 2500`): was "AP"
- `NKI/2/bold_TE=645.json` <- `NKI_2/scantable.pdf` (select `*Rest 645`): was "AP"
- `NKI/2/dwi.json` <- `NKI_2/scantable.pdf` (select `*DTI`): was "AP"
- `NYU/2/T1w.json` <- `NYU_2/anat.pdf`: was "AP"
- `SWU/1/T1w.json` <- `SWU_1/scantable.pdf` (select `*Anatomical`): was "AP"
- `SWU/1/bold.json` <- `SWU_1/scantable.pdf` (select `*Rest`): was "AP"
- `SWU/2/T1w.json` <- `SWU_2/scantable.pdf` (select `*Anatomical`): was "AP"
- `SWU/2/bold.json` <- `SWU_2/scantable.pdf` (select `*Rest`): was "AP"
- `SWU/3/T1w.json` <- `SWU_3/scantable.pdf` (select `*Anatomical`): was "AP"
- `SWU/3/bold.json` <- `SWU_3/scantable.pdf` (select `*Rest`): was "AP"
- `SWU/4/T1w.json` <- `SWU_4/scantable.pdf` (select `*Anatomical`): was "AP"
- `SWU/4/bold.json` <- `SWU_4/scantable.pdf` (select `*Rest`): was "AP"
- `UM/1/T1w.json` <- `UM_1/all.pdf` (select `1`): was "AP"
- `UPSM/1/T1w.json` <- `UPSM_1/scantable.pdf` (select `*Anatomical`): was "RL"
- `UPSM/1/bold.json` <- `UPSM_1/scantable.pdf` (select `*Rest`): was "PA"
- `Utah/1/T1w.json` <- `Utah_1/anat.pdf`: was "AP"
- `Utah/2/T1w.json` <- `Utah_2/anat.pdf`: was "AP"

### `SliceTiming` dropped (46)

- `BMB/1/T1w.json` <- `BMB_1/anat.pdf`: was "Interleaved Ascending"
- `BNU/1/T1w.json` <- `BNU_1/anat.pdf`: was "Interleaved Ascending"
- `BNU/2/T1w.json` <- `BNU_2/anat1.pdf`: was "Interleaved Ascending"
- `BNU/3/T1w.json` <- `BNU_3/anat.pdf`: was "Interleaved Ascending"
- `HNU/1/T1w.json` <- `HNU_1/scantable.pdf` (select `*Anatomical`): was "Interleaved Ascending"
- `HNU/1/bold.json` <- `HNU_1/scantable.pdf` (select `*Rest`): was "Interleaved Ascending"
- `HNU/1/dwi.json` <- `HNU_1/scantable.pdf` (select `*DTI`): was "Interleaved Ascending"
- `IACAS/1/T1w.json` <- `IACAS_1/anat.pdf`: was "Sequential Ascending"
- `IBA/TRT/T1w.json` <- `IBA_TRT/scantable.pdf` (select `*Anatomical`): was "Sequential Ascending"
- `IBA/TRT/bold.json` <- `IBA_TRT/scantable.pdf` (select `*Rest`): was "Sequential Ascending"
- `IPCAS/1/T1w.json` <- `IPCAS_1/scantable.pdf` (select `*Anatomical`): was "Sequential Ascending"
- `IPCAS/2/T1w.json` <- `IPCAS_2/scantable.pdf` (select `*Anatomical`): was "Sequential Ascending"
- `IPCAS/3/T1w.json` <- `IPCAS_3/scantable.pdf` (select `*Anatomical`): was "Interleaved Ascending"
- `IPCAS/3/bold.json` <- `IPCAS_3/scantable.pdf` (select `*Rest`): was "Interleaved Ascending"
- `IPCAS/4/T1w.json` <- `IPCAS_4/scantable.pdf` (select `*Anatomical`): was "Interleaved Ascending"
- `IPCAS/4/bold.json` <- `IPCAS_4/scantable.pdf` (select `*Rest`): was "Interleaved Ascending"
- `IPCAS/5/T1w.json` <- `IPCAS_5/all.pdf` (select `3`): was "Interleaved Ascending"
- `IPCAS/6/T1w.json` <- `IPCAS_6/scantable.pdf` (select `*Anatomical`): was "Sequential Ascending"
- `IPCAS/7/T1w.json` <- `IPCAS_7/scantable.pdf` (select `*Anatomical`): was "Sequential Ascending"
- `IPCAS/8/T1w.json` <- `IPCAS_8/scantable.pdf` (select `*Anatomical`): was "Interleaved Ascending"
- `IPCAS/8/dwi.json` <- `IPCAS_8/scantable.pdf` (select `*DTI`): was "Interleaved Ascending"
- `JHNU/1/T1w.json` <- `JHNU_1/scantable.pdf` (select `*Anatomical`): was "Sequential Ascending"
- `LMU/1/T1w.json` <- `LMU_1/scantable.pdf` (select `*Anatomical`): was "Sequential Ascending"
- `LMU/1/bold.json` <- `LMU_1/scantable.pdf` (select `*Rest`): was "Sequential Ascending"
- `LMU/2/T1w.json` <- `LMU_2/scantable.pdf` (select `*Anatomical`): was "Interleaved Ascending"
- `LMU/2/bold.json` <- `LMU_2/scantable.pdf` (select `*Rest`): was "Interleaved Ascending"
- `LMU/3/T1w.json` <- `LMU_3/scantable.pdf` (select `*Anatomical`): was "Interleaved Ascending"
- `LMU/3/bold.json` <- `LMU_3/scantable.pdf` (select `*Rest`): was "Interleaved Ascending"
- `MPG/1/T1w.json` <- `MPG_1/scantable.pdf` (select `*Anatomical`): was "Interleaved Ascending"
- `MPG/1/bold.json` <- `MPG_1/scantable.pdf` (select `*Rest`): was "Interleaved Ascending"
- `MRN/1/T1w.json` <- `MRN_1/scantable.pdf` (select `*Anatomical`): was "Interleaved Ascending"
- `MRN/1/dwi.json` <- `MRN_1/scantable.pdf` (select `*DTI`): was "Interleaved Ascending"
- `NKI/2/T1w.json` <- `NKI_2/scantable.pdf` (select `*Anatomical`): was "Sequential Ascending"
- `NKI/2/bold_TE=2500.json` <- `NKI_2/scantable.pdf` (select `*Rest 2500`): was "Interleaved Ascending"
- `NKI/2/bold_TE=645.json` <- `NKI_2/scantable.pdf` (select `*Rest 645`): was "Interleaved Ascending"
- `NKI/2/dwi.json` <- `NKI_2/scantable.pdf` (select `*DTI`): was "Interleaved Ascending"
- `NYU/2/T1w.json` <- `NYU_2/anat.pdf`: was "Sequential Ascending"
- `SWU/1/T1w.json` <- `SWU_1/scantable.pdf` (select `*Anatomical`): was "Sequential Ascending"
- `SWU/2/T1w.json` <- `SWU_2/scantable.pdf` (select `*Anatomical`): was "Sequential Ascending"
- `SWU/3/T1w.json` <- `SWU_3/scantable.pdf` (select `*Anatomical`): was "Sequential Ascending"
- `SWU/4/T1w.json` <- `SWU_4/scantable.pdf` (select `*Anatomical`): was "Sequential Ascending"
- `UM/1/T1w.json` <- `UM_1/all.pdf` (select `1`): was "Interleaved Ascending"
- `UPSM/1/T1w.json` <- `UPSM_1/scantable.pdf` (select `*Anatomical`): was "Interleaved Ascending"
- `UPSM/1/bold.json` <- `UPSM_1/scantable.pdf` (select `*Rest`): was "Sequential Ascending"
- `Utah/1/T1w.json` <- `Utah_1/anat.pdf`: was "Interleaved Ascending"
- `Utah/2/T1w.json` <- `Utah_2/anat.pdf`: was "Interleaved Ascending"

### `RepetitionTimeExcitation` dropped (35)

- `BMB/1/bold.json` <- `BMB_1/rest.pdf`: was 2.3000000000000003
- `BNU/1/bold.json` <- `BNU_1/rest.pdf`: was 2.0
- `BNU/2/bold.json` <- `BNU_2/rest1.pdf`: was 2.0
- `BNU/3/bold.json` <- `BNU_3/rest.pdf`: was 2.0
- `DC/1/bold.json` <- `DC_1/scantable.pdf` (select `*Rest`): was 2.5
- `HNU/1/bold.json` <- `HNU_1/scantable.pdf` (select `*Rest`): was 2.0
- `IACAS/1/bold.json` <- `IACAS_1/rest.pdf`: was 2.0
- `IBA/TRT/bold.json` <- `IBA_TRT/scantable.pdf` (select `*Rest`): was 1.75
- `IPCAS/1/bold.json` <- `IPCAS_1/scantable.pdf` (select `*Rest`): was 2.0
- `IPCAS/2/bold.json` <- `IPCAS_2/scantable.pdf` (select `*Rest`): was 2.5
- `IPCAS/3/bold.json` <- `IPCAS_3/scantable.pdf` (select `*Rest`): was 2.0
- `IPCAS/4/bold.json` <- `IPCAS_4/scantable.pdf` (select `*Rest`): was 2.0
- `IPCAS/5/bold.json` <- `IPCAS_5/all.pdf` (select `1`): was 2.0
- `IPCAS/6/bold.json` <- `IPCAS_6/scantable.pdf` (select `*Rest`): was 2.5
- `IPCAS/7/bold.json` <- `IPCAS_7/scantable.pdf` (select `*Rest`): was 2.5
- `IPCAS/8/bold.json` <- `IPCAS_8/scantable.pdf` (select `*Rest`): was 2.0
- `JHNU/1/bold.json` <- `JHNU_1/scantable.pdf` (select `*Rest`): was 2.0
- `LMU/1/bold.json` <- `LMU_1/scantable.pdf` (select `*Rest`): was 2.5
- `LMU/2/bold.json` <- `LMU_2/scantable.pdf` (select `*Rest`): was 3.0
- `LMU/3/bold.json` <- `LMU_3/scantable.pdf` (select `*Rest`): was 3.0
- `MPG/1/bold.json` <- `MPG_1/scantable.pdf` (select `*Rest`): was 3.0
- `MRN/1/bold.json` <- `MRN_1/scantable.pdf` (select `*Rest`): was 2.0
- `NKI/2/bold_TE=1400.json` <- `NKI_2/scantable.pdf` (select `*Rest 1400`): was 1.4000000000000001
- `NKI/2/bold_TE=2500.json` <- `NKI_2/scantable.pdf` (select `*Rest 2500`): was 2.5
- `NKI/2/bold_TE=645.json` <- `NKI_2/scantable.pdf` (select `*Rest 645`): was 0.645
- `NYU/1/bold.json` <- `NYU_1/scantable.pdf` (select `*Rest`): was 2.0
- `NYU/2/bold.json` <- `NYU_2/rest.pdf`: was 2.0
- `SWU/1/bold.json` <- `SWU_1/scantable.pdf` (select `*Rest`): was 2.0
- `SWU/2/bold.json` <- `SWU_2/scantable.pdf` (select `*Rest`): was 2.0
- `SWU/3/bold.json` <- `SWU_3/scantable.pdf` (select `*Rest`): was 2.0
- `SWU/4/bold.json` <- `SWU_4/scantable.pdf` (select `*Rest`): was 2.0
- `UM/1/bold.json` <- `UM_1/all.pdf` (select `0`): was 2.0
- `UPSM/1/bold.json` <- `UPSM_1/scantable.pdf` (select `*Rest`): was 1.5
- `Utah/1/bold.json` <- `Utah_1/rest.pdf`: was 2.0
- `Utah/2/bold.json` <- `Utah_2/rest.pdf`: was 2.0

### `EffectiveEchoSpacing` dropped (28)

- `DC/1/bold.json` <- `DC_1/scantable.pdf` (select `*Rest`): was 3.4482758620689654e-06
- `HNU/1/bold.json` <- `HNU_1/scantable.pdf` (select `*Rest`): was 4.5454545454545455e-06
- `IACAS/1/bold.json` <- `IACAS_1/rest.pdf`: was 2e-06
- `IBA/TRT/bold.json` <- `IBA_TRT/scantable.pdf` (select `*Rest`): was 6.398443898443899e-06
- `IPCAS/1/bold.json` <- `IPCAS_1/scantable.pdf` (select `*Rest`): was 7.000448028673835e-06
- `IPCAS/2/bold.json` <- `IPCAS_2/scantable.pdf` (select `*Rest`): was 7.000448028673835e-06
- `IPCAS/3/bold.json` <- `IPCAS_3/scantable.pdf` (select `*Rest`): was 7.000448028673835e-06
- `IPCAS/4/bold.json` <- `IPCAS_4/scantable.pdf` (select `*Rest`): was 6.25e-05
- `IPCAS/6/bold.json` <- `IPCAS_6/scantable.pdf` (select `*Rest`): was 6.799390774586597e-06
- `IPCAS/7/bold.json` <- `IPCAS_7/scantable.pdf` (select `*Rest`): was 6.200396825396825e-06
- `IPCAS/8/bold.json` <- `IPCAS_8/scantable.pdf` (select `*Rest`): was 6.200396825396825e-06
- `IPCAS/8/dwi.json` <- `IPCAS_8/scantable.pdf` (select `*DTI`): was 5.59634670487106e-06
- `JHNU/1/bold.json` <- `JHNU_1/scantable.pdf` (select `*Rest`): was 7.006726457399103e-06
- `LMU/1/bold.json` <- `LMU_1/scantable.pdf` (select `*Rest`): was 6.475341898052217e-06
- `LMU/2/bold.json` <- `LMU_2/scantable.pdf` (select `*Rest`): was 7.000448028673835e-06
- `LMU/3/bold.json` <- `LMU_3/scantable.pdf` (select `*Rest`): was 7.000448028673835e-06
- `MPG/1/bold.json` <- `MPG_1/scantable.pdf` (select `*Rest`): was 7.000448028673835e-06
- `MRN/1/bold.json` <- `MRN_1/scantable.pdf` (select `*Rest`): was 7.2004608294930876e-06
- `MRN/1/dwi.json` <- `MRN_1/scantable.pdf` (select `*DTI`): was 5.001600512163892e-06
- `NKI/2/bold_TE=1400.json` <- `NKI_2/scantable.pdf` (select `*Rest 1400`): was 4.999200127979523e-06
- `NKI/2/bold_TE=2500.json` <- `NKI_2/scantable.pdf` (select `*Rest 2500`): was 6.200396825396825e-06
- `NKI/2/bold_TE=645.json` <- `NKI_2/scantable.pdf` (select `*Rest 645`): was 5.201506356240767e-06
- `NKI/2/dwi.json` <- `NKI_2/scantable.pdf` (select `*DTI`): was 5.200640718936573e-06
- `SWU/1/bold.json` <- `SWU_1/scantable.pdf` (select `*Rest`): was 7.000448028673835e-06
- `SWU/2/bold.json` <- `SWU_2/scantable.pdf` (select `*Rest`): was 7.000448028673835e-06
- `SWU/3/bold.json` <- `SWU_3/scantable.pdf` (select `*Rest`): was 7.000448028673835e-06
- `SWU/4/bold.json` <- `SWU_4/scantable.pdf` (select `*Rest`): was 7.000448028673835e-06
- `UPSM/1/bold.json` <- `UPSM_1/scantable.pdf` (select `*Rest`): was 5.79992576095026e-06

### `RepetitionTimePreparation` dropped (24)

- `DC/1/T1w.json` <- `DC_1/scantable.pdf` (select `*Anatomical`): was 2.375
- `HNU/1/T1w.json` <- `HNU_1/scantable.pdf` (select `*Anatomical`): was 0.008060000000000001
- `IACAS/1/T1w.json` <- `IACAS_1/anat.pdf`: was 0.007788000000000001
- `IBA/TRT/T1w.json` <- `IBA_TRT/scantable.pdf` (select `*Anatomical`): was 2.6
- `IPCAS/1/T1w.json` <- `IPCAS_1/scantable.pdf` (select `*Anatomical`): was 2.5300000000000002
- `IPCAS/2/T1w.json` <- `IPCAS_2/scantable.pdf` (select `*Anatomical`): was 2.3000000000000003
- `IPCAS/3/T1w.json` <- `IPCAS_3/scantable.pdf` (select `*Anatomical`): was 2.5300000000000002
- `IPCAS/4/T1w.json` <- `IPCAS_4/scantable.pdf` (select `*Anatomical`): was 8.068
- `IPCAS/6/T1w.json` <- `IPCAS_6/scantable.pdf` (select `*Anatomical`): was 1.9000000000000001
- `IPCAS/7/T1w.json` <- `IPCAS_7/scantable.pdf` (select `*Anatomical`): was 2.6
- `IPCAS/8/T1w.json` <- `IPCAS_8/scantable.pdf` (select `*Anatomical`): was 2.5300000000000002
- `JHNU/1/T1w.json` <- `JHNU_1/scantable.pdf` (select `*Anatomical`): was 2.3000000000000003
- `LMU/1/T1w.json` <- `LMU_1/scantable.pdf` (select `*Anatomical`): was 2.375
- `LMU/2/T1w.json` <- `LMU_2/scantable.pdf` (select `*Anatomical`): was 2.4
- `LMU/3/T1w.json` <- `LMU_3/scantable.pdf` (select `*Anatomical`): was 2.4
- `MPG/1/T1w.json` <- `MPG_1/scantable.pdf` (select `*Anatomical`): was 5.0
- `MRN/1/T1w.json` <- `MRN_1/scantable.pdf` (select `*Anatomical`): was 2.5300000000000002
- `NKI/2/T1w.json` <- `NKI_2/scantable.pdf` (select `*Anatomical`): was 1.9000000000000001
- `NYU/1/T1w.json` <- `NYU_1/scantable.pdf` (select `*Anatomical`): was 2.5
- `SWU/1/T1w.json` <- `SWU_1/scantable.pdf` (select `*Anatomical`): was 1.9000000000000001
- `SWU/2/T1w.json` <- `SWU_2/scantable.pdf` (select `*Anatomical`): was 1.9000000000000001
- `SWU/3/T1w.json` <- `SWU_3/scantable.pdf` (select `*Anatomical`): was 1.9000000000000001
- `SWU/4/T1w.json` <- `SWU_4/scantable.pdf` (select `*Anatomical`): was 1.9000000000000001
- `UPSM/1/T1w.json` <- `UPSM_1/scantable.pdf` (select `*Anatomical`): was 2.1

### `DwellTime` dropped (23)

- `DC/1/T1w.json` <- `DC_1/scantable.pdf` (select `*Anatomical`): was 2.176941832114246e-05
- `HNU/1/T1w.json` <- `HNU_1/scantable.pdf` (select `*Anatomical`): was 3.2e-05
- `IACAS/1/T1w.json` <- `IACAS_1/anat.pdf`: was 3.201844262295082e-05
- `IBA/TRT/T1w.json` <- `IBA_TRT/scantable.pdf` (select `*Anatomical`): was 3.0048076923076925e-05
- `IPCAS/1/T1w.json` <- `IPCAS_1/scantable.pdf` (select `*Anatomical`): was 2.2977941176470588e-05
- `IPCAS/2/T1w.json` <- `IPCAS_2/scantable.pdf` (select `*Anatomical`): was 3.205128205128205e-05
- `IPCAS/3/T1w.json` <- `IPCAS_3/scantable.pdf` (select `*Anatomical`): was 2.2977941176470588e-05
- `IPCAS/4/T1w.json` <- `IPCAS_4/scantable.pdf` (select `*Anatomical`): was 0.000128
- `IPCAS/6/T1w.json` <- `IPCAS_6/scantable.pdf` (select `*Anatomical`): was 2.2977941176470588e-05
- `IPCAS/7/T1w.json` <- `IPCAS_7/scantable.pdf` (select `*Anatomical`): was 2.170138888888889e-05
- `IPCAS/8/T1w.json` <- `IPCAS_8/scantable.pdf` (select `*Anatomical`): was 2.055921052631579e-05
- `JHNU/1/T1w.json` <- `JHNU_1/scantable.pdf` (select `*Anatomical`): was 1.6276041666666666e-05
- `LMU/1/T1w.json` <- `LMU_1/scantable.pdf` (select `*Anatomical`): was 2.175805047867711e-05
- `LMU/2/T1w.json` <- `LMU_2/scantable.pdf` (select `*Anatomical`): was 1.6983695652173913e-05
- `LMU/3/T1w.json` <- `LMU_3/scantable.pdf` (select `*Anatomical`): was 1.6983695652173913e-05
- `MPG/1/T1w.json` <- `MPG_1/scantable.pdf` (select `*Anatomical`): was 1.25e-05
- `MRN/1/T1w.json` <- `MRN_1/scantable.pdf` (select `*Anatomical`): was 6.000384024577573e-06
- `NKI/2/T1w.json` <- `NKI_2/scantable.pdf` (select `*Anatomical`): was 2.2977941176470588e-05
- `SWU/1/T1w.json` <- `SWU_1/scantable.pdf` (select `*Anatomical`): was 2.2977941176470588e-05
- `SWU/2/T1w.json` <- `SWU_2/scantable.pdf` (select `*Anatomical`): was 2.2977941176470588e-05
- `SWU/3/T1w.json` <- `SWU_3/scantable.pdf` (select `*Anatomical`): was 2.2977941176470588e-05
- `SWU/4/T1w.json` <- `SWU_4/scantable.pdf` (select `*Anatomical`): was 2.2977941176470588e-05
- `UPSM/1/T1w.json` <- `UPSM_1/scantable.pdf` (select `*Anatomical`): was 1.6276041666666666e-05

### `EffectiveEchoSpacing` differs (11)

- `BMB/1/bold.json` <- `BMB_1/rest.pdf`: 7.00045e-06 -> 0.00051 (x72.85)
- `BNU/1/bold.json` <- `BNU_1/rest.pdf`: 6.2004e-06 -> 0.00046 (x74.19)
- `BNU/1/dwi.json` <- `BNU_1/dti.pdf`: 5.0016e-06 -> 0.00037 (x73.98)
- `BNU/2/bold.json` <- `BNU_2/rest1.pdf`: 6.2004e-06 -> 0.00046 (x74.19)
- `BNU/3/bold.json` <- `BNU_3/rest.pdf`: 6.2004e-06 -> 0.00046 (x74.19)
- `BNU/3/dwi.json` <- `BNU_3/dti.pdf`: 5.59635e-06 -> 0.00041 (x73.26)
- `IPCAS/5/bold.json` <- `IPCAS_5/all.pdf` (select `1`): 6.79939e-06 -> 0.00046 (x67.65)
- `NYU/2/bold.json` <- `NYU_2/rest.pdf`: 3.2002e-06 -> 0.00032 (x99.99)
- `UM/1/bold.json` <- `UM_1/all.pdf` (select `0`): 6.39844e-06 -> 0.00047 (x73.46)
- `Utah/1/bold.json` <- `Utah_1/rest.pdf`: 5.3991e-06 -> 0.000215 (x39.82)
- `Utah/2/bold.json` <- `Utah_2/rest.pdf`: 5.3991e-06 -> 0.000215 (x39.82)

### `DwellTime` factor-2 difference (7)

- `BMB/1/T1w.json` <- `BMB_1/anat.pdf`: 1.6276e-05 -> 8.138e-06 (x0.50)
- `BNU/3/T1w.json` <- `BNU_3/anat.pdf`: 2.05592e-05 -> 1.028e-05 (x0.50)
- `IPCAS/5/T1w.json` <- `IPCAS_5/all.pdf` (select `3`): 2.05592e-05 -> 1.028e-05 (x0.50)
- `NYU/2/T1w.json` <- `NYU_2/anat.pdf`: 1.95313e-05 -> 9.766e-06 (x0.50)
- `UM/1/T1w.json` <- `UM_1/all.pdf` (select `1`): 1.6276e-05 -> 8.138e-06 (x0.50)
- `Utah/1/T1w.json` <- `Utah_1/anat.pdf`: 1.6276e-05 -> 8.138e-06 (x0.50)
- `Utah/2/T1w.json` <- `Utah_2/anat.pdf`: 1.6276e-05 -> 8.138e-06 (x0.50)

### no repetition time produced (4)

- `IACAS/1/T1w.json` <- `IACAS_1/anat.pdf`
- `IPCAS/1/dwi.json` <- `IPCAS_1/scantable.pdf` (select `*DTI`)
- `IPCAS/2/dwi.json` <- `IPCAS_2/scantable.pdf` (select `*DTI`)
- `SWU/4/dwi.json` <- `SWU_4/scantable.pdf` (select `*DTI`)

### `MRAcquisitionType` "2D" for a 3D anatomical (3)

- `IPCAS/1/T1w.json` <- `IPCAS_1/scantable.pdf` (select `*Anatomical`): was 3D
- `IPCAS/2/T1w.json` <- `IPCAS_2/scantable.pdf` (select `*Anatomical`): was 3D
- `NYU/1/T1w.json` <- `NYU_1/scantable.pdf` (select `*Anatomical`): was 3D

### `DwellTime` differs (2)

- `BNU/1/T1w.json` <- `BNU_1/anat.pdf`: 1.52588e-05 -> 1.028e-05 (x0.67)
- `BNU/2/T1w.json` <- `BNU_2/anat1.pdf`: 1.52588e-05 -> 1.028e-05 (x0.67)

### `EchoTime` dropped (1)

- `IACAS/1/T1w.json` <- `IACAS_1/anat.pdf`: was 0.002984

### `ParallelReductionFactorInPlane` dropped (1)

- `IPCAS/5/T1w.json` <- `IPCAS_5/all.pdf` (select `3`): was 2.0

### `EchoTime` differs (1)

- `IPCAS/5/T1w.json` <- `IPCAS_5/all.pdf` (select `3`): 0.0035 -> 0.0033900000000000002

## Non-BIDS keys dropped from the old templates (not re-added)

None.

protocol2bids 0.0.0 (commit 5b826707154c64f1cf256572f00a158a54e3da27)

Generated by `scripts/make_sidecars.py`; diffs are against the sidecar at git HEAD when the script was run.

## BMB/1/T1w.json

- added `ManufacturersModelName`: "TrioTim"
- added `SoftwareVersions`: "syngo MR B15"
- added `ReceiveCoilActiveElements`: "HEA;HEP"
- added `MatrixCoilMode`: "Auto (CP)"
- added `CoilCombinationMethod`: "Adaptive Combine"
- added `ScanningSequence`: ["GR", "IR"]
- added `SequenceVariant`: ["SK", "SP", "MP"]
- added `ScanOptions`: ["IR"]
- added `SequenceName`: "tfl"
- added `PulseSequenceDetails`: "%SiemensSeq%\\tfl"
- added `SpoilingState`: true
- added `SpoilingType`: "COMBINED"
- added `SpoilingRFPhaseIncrement`: 50.0
- added `B0ShimmingTechnique`: "Standard"
- added `RepetitionTime`: 2.3000000000000003
- added `RepetitionTimeExcitation`: 0.0070999999999999995
- added `Modality`: "MR"
- added `ProtocolName`: "mprage 1 mm iso"
- added `SeriesDescription`: "mprage 1 mm iso"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 256
- added `AcquisitionMatrixPE`: 256
- added `ReconMatrixPE`: 256
- added `PercentPhaseFOV`: 100.0
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 256
- added `PixelBandwidth`: 240.0
- added `SliceThickness`: 1.0
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceTiming`: "Interleaved Ascending"
- changed `SliceEncodingDirection`: "S" -> "i"
- changed `DwellTime`: 1.6276041666666666e-05 -> 8.138e-06

## BMB/1/bold.json

- added `ManufacturersModelName`: "TrioTim"
- added `SoftwareVersions`: "syngo MR B15"
- added `ReceiveCoilActiveElements`: "BC"
- added `MatrixCoilMode`: "Auto (CP)"
- added `CoilCombinationMethod`: "rSOS"
- added `ScanningSequence`: ["EP"]
- added `SequenceVariant`: ["SK"]
- added `ScanOptions`: ["FS"]
- added `SequenceName`: "ep2d_bold"
- added `PulseSequenceDetails`: "%SiemensSeq%\\ep2d_bold"
- added `MRAcquisitionType`: "2D"
- added `MTState`: false
- added `B0ShimmingTechnique`: "Standard"
- added `NumberShots`: 1
- added `TotalReadoutTime`: 0.032130000000000006
- added `DwellTime`: 3.5000000000000004e-06
- added `RepetitionTime`: 2.3000000000000003
- added `Modality`: "MR"
- added `ProtocolName`: "BOLD resting state"
- added `SeriesDescription`: "BOLD resting state"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 64
- added `AcquisitionMatrixPE`: 64
- added `ReconMatrixPE`: 64
- added `PercentPhaseFOV`: 100.0
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 64
- added `EchoTrainLength`: 64
- added `PixelBandwidth`: 2232.0
- added `BandwidthPerPixelPhaseEncode`: 30.637254901960784
- added `DerivedVendorReportedEchoSpacing`: 0.00051
- added `SliceThickness`: 4.0
- added `SpacingBetweenSlices`: 4.0
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TrioTim"
- removed `RepetitionTimeExcitation`: 2.3000000000000003
- changed `PulseSequenceType`: "EPI" -> "Gradient Echo EPI"
- changed `PhaseEncodingDirection`: "AP" -> "j-"
- changed `EffectiveEchoSpacing`: 7.000448028673835e-06 -> 0.00051
- changed `SliceTiming`: "Interleaved Ascending" -> [1.15, 0.0, 1.217647, 0.067647, 1.285294, 0.135294, 1.352941, 0.202941, 1.420588, 0.270588, 1.488235, 0.338235, 1.555882, 0.405882, 1.623529, 0.473529, 1.691176, 0.541176, 1.758824, 0.608824, 1.826471, 0.676471, 1.894118, 0.744118, 1.961765, 0.811765, 2.029412, 0.879412, 2.097059, 0.947059, 2.164706, 1.014706, 2.232353, 1.082353]
- changed `SliceEncodingDirection`: "A" -> "k"

## BNU/1/T1w.json

- added `ManufacturersModelName`: "TrioTim"
- added `SoftwareVersions`: "syngo MR B17"
- added `ReceiveCoilActiveElements`: "HEA;HEP"
- added `MatrixCoilMode`: "Auto (CP)"
- added `CoilCombinationMethod`: "Adaptive Combine"
- added `ScanningSequence`: ["GR", "IR"]
- added `SequenceVariant`: ["SK", "SP", "MP"]
- added `ScanOptions`: ["IR"]
- added `SequenceName`: "tfl"
- added `PulseSequenceDetails`: "%SiemensSeq%\\tfl"
- added `SpoilingState`: true
- added `SpoilingType`: "COMBINED"
- added `SpoilingRFPhaseIncrement`: 50.0
- added `B0ShimmingTechnique`: "Tune up"
- added `RepetitionTime`: 2.5300000000000002
- added `RepetitionTimeExcitation`: 0.0078
- added `Modality`: "MR"
- added `ProtocolName`: "t1_mprage_sag_144"
- added `SeriesDescription`: "t1_mprage_sag_144"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 256
- added `AcquisitionMatrixPE`: 192
- added `ReconMatrixPE`: 256
- added `PercentPhaseFOV`: 100.0
- added `PercentSampling`: 75.0
- added `PhaseResolution`: 0.75
- added `PhaseEncodingSteps`: 192
- added `PixelBandwidth`: 190.0
- added `SliceThickness`: 1.33
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceTiming`: "Interleaved Ascending"
- changed `SliceEncodingDirection`: "S" -> "i"
- changed `DwellTime`: 1.52587890625e-05 -> 1.028e-05

## BNU/1/bold.json

- added `ManufacturersModelName`: "TrioTim"
- added `SoftwareVersions`: "syngo MR B17"
- added `ReceiveCoilActiveElements`: "HEA;HEP"
- added `MatrixCoilMode`: "Auto (CP)"
- added `CoilCombinationMethod`: "rSOS"
- added `ScanningSequence`: ["EP"]
- added `SequenceVariant`: ["SK"]
- added `ScanOptions`: ["FS"]
- added `SequenceName`: "ep2d_bold"
- added `PulseSequenceDetails`: "%SiemensSeq%\\ep2d_bold"
- added `MRAcquisitionType`: "2D"
- added `MTState`: false
- added `B0ShimmingTechnique`: "Standard"
- added `NumberShots`: 1
- added `TotalReadoutTime`: 0.028980000000000002
- added `DwellTime`: 3.1000000000000004e-06
- added `RepetitionTime`: 2.0
- added `Modality`: "MR"
- added `ProtocolName`: "ge_func_31x31x35_200_RS"
- added `SeriesDescription`: "ge_func_31x31x35_200_RS"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 64
- added `AcquisitionMatrixPE`: 64
- added `ReconMatrixPE`: 64
- added `PercentPhaseFOV`: 100.0
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 64
- added `EchoTrainLength`: 64
- added `PixelBandwidth`: 2520.0
- added `BandwidthPerPixelPhaseEncode`: 33.96739130434783
- added `DerivedVendorReportedEchoSpacing`: 0.00046
- added `SliceThickness`: 3.5
- added `SpacingBetweenSlices`: 4.2
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TrioTim"
- removed `RepetitionTimeExcitation`: 2.0
- changed `PulseSequenceType`: "EPI" -> "Gradient Echo EPI"
- changed `PhaseEncodingDirection`: "AP" -> "j-"
- changed `EffectiveEchoSpacing`: 6.200396825396825e-06 -> 0.00046
- changed `SliceTiming`: "Interleaved Ascending" -> [0.0, 1.030303, 0.060606, 1.090909, 0.121212, 1.151515, 0.181818, 1.212121, 0.242424, 1.272727, 0.30303, 1.333333, 0.363636, 1.393939, 0.424242, 1.454545, 0.484848, 1.515152, 0.545455, 1.575758, 0.606061, 1.636364, 0.666667, 1.69697, 0.727273, 1.757576, 0.787879, 1.818182, 0.848485, 1.878788, 0.909091, 1.939394, 0.969697]
- changed `SliceEncodingDirection`: "A" -> "k"

## BNU/1/dwi.json

- added `ManufacturersModelName`: "TrioTim"
- added `SoftwareVersions`: "syngo MR B17"
- added `ReceiveCoilActiveElements`: "HEA;HEP"
- added `MatrixCoilMode`: "Auto (Triple)"
- added `CoilCombinationMethod`: "Adaptive Combine"
- added `ScanningSequence`: ["EP"]
- added `SequenceVariant`: ["SK", "SP"]
- added `ScanOptions`: ["PFP", "FS"]
- added `SequenceName`: "ep2d_diff"
- added `PulseSequenceDetails`: "%SiemensSeq%\\ep2d_diff"
- added `MRAcquisitionType`: "2D"
- added `MTState`: false
- added `B0ShimmingTechnique`: "Standard"
- added `NumberShots`: 1
- added `PartialFourierDirection`: "PHASE"
- added `TotalReadoutTime`: 0.04699
- added `DwellTime`: 2.5010000000000003e-06
- added `RepetitionTime`: 8.0
- added `Modality`: "MR"
- added `ProtocolName`: "DTI_30_2p2x2p2x2p2_6-8_p2"
- added `SeriesDescription`: "DTI_30_2p2x2p2x2p2_6-8_p2"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 128
- added `AcquisitionMatrixPE`: 128
- added `ReconMatrixPE`: 128
- added `PercentPhaseFOV`: 100.0
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 96
- added `EchoTrainLength`: 48
- added `PixelBandwidth`: 1562.0
- added `BandwidthPerPixelPhaseEncode`: 21.114864864864867
- added `DerivedVendorReportedEchoSpacing`: 0.00074
- added `SliceThickness`: 2.0
- added `SpacingBetweenSlices`: 2.0
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TrioTim"
- changed `PulseSequenceType`: "EPI" -> "Spin Echo EPI"
- changed `PhaseEncodingDirection`: "AP" -> "j-"
- changed `EffectiveEchoSpacing`: 5.001600512163892e-06 -> 0.00037
- changed `SliceTiming`: "Interleaved Ascending" -> [4.0, 0.0, 4.129032, 0.129032, 4.258065, 0.258065, 4.387097, 0.387097, 4.516129, 0.516129, 4.645161, 0.645161, 4.774194, 0.774194, 4.903226, 0.903226, 5.032258, 1.032258, 5.16129, 1.16129, 5.290323, 1.290323, 5.419355, 1.419355, 5.548387, 1.548387, 5.677419, 1.677419, 5.806452, 1.806452, 5.935484, 1.935484, 6.064516, 2.064516, 6.193548, 2.193548, 6.322581, 2.322581, 6.451613, 2.451613, 6.580645, 2.580645, 6.709677, 2.709677, 6.83871, 2.83871, 6.967742, 2.967742, 7.096774, 3.096774, 7.225806, 3.225806, 7.354839, 3.354839, 7.483871, 3.483871, 7.612903, 3.612903, 7.741935, 3.741935, 7.870968, 3.870968]
- changed `SliceEncodingDirection`: "a" -> "k"

## BNU/2/T1w.json

- added `ManufacturersModelName`: "TrioTim"
- added `SoftwareVersions`: "syngo MR B17"
- added `ReceiveCoilActiveElements`: "HEA;HEP"
- added `MatrixCoilMode`: "Auto (CP)"
- added `CoilCombinationMethod`: "Adaptive Combine"
- added `ScanningSequence`: ["GR", "IR"]
- added `SequenceVariant`: ["SK", "SP", "MP"]
- added `ScanOptions`: ["IR"]
- added `SequenceName`: "tfl"
- added `PulseSequenceDetails`: "%SiemensSeq%\\tfl"
- added `SpoilingState`: true
- added `SpoilingType`: "COMBINED"
- added `SpoilingRFPhaseIncrement`: 50.0
- added `B0ShimmingTechnique`: "Tune up"
- added `RepetitionTime`: 2.5300000000000002
- added `RepetitionTimeExcitation`: 0.0078
- added `Modality`: "MR"
- added `ProtocolName`: "t1_mprage_sag"
- added `SeriesDescription`: "t1_mprage_sag"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 256
- added `AcquisitionMatrixPE`: 192
- added `ReconMatrixPE`: 256
- added `PercentPhaseFOV`: 100.0
- added `PercentSampling`: 75.0
- added `PhaseResolution`: 0.75
- added `PhaseEncodingSteps`: 192
- added `PixelBandwidth`: 190.0
- added `SliceThickness`: 1.33
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceTiming`: "Interleaved Ascending"
- changed `SliceEncodingDirection`: "S" -> "i"
- changed `DwellTime`: 1.52587890625e-05 -> 1.028e-05

## BNU/2/bold.json

- added `ManufacturersModelName`: "TrioTim"
- added `SoftwareVersions`: "syngo MR B17"
- added `ReceiveCoilActiveElements`: "HEA;HEP"
- added `MatrixCoilMode`: "Auto (CP)"
- added `CoilCombinationMethod`: "rSOS"
- added `ScanningSequence`: ["EP"]
- added `SequenceVariant`: ["SK"]
- added `ScanOptions`: ["FS"]
- added `SequenceName`: "ep2d_bold"
- added `PulseSequenceDetails`: "%SiemensSeq%\\ep2d_bold"
- added `MRAcquisitionType`: "2D"
- added `MTState`: false
- added `B0ShimmingTechnique`: "Standard"
- added `NumberShots`: 1
- added `TotalReadoutTime`: 0.028980000000000002
- added `DwellTime`: 3.1000000000000004e-06
- added `RepetitionTime`: 2.0
- added `Modality`: "MR"
- added `ProtocolName`: "ge_func_31x31x30_240_RS"
- added `SeriesDescription`: "ge_func_31x31x30_240_RS"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 64
- added `AcquisitionMatrixPE`: 64
- added `ReconMatrixPE`: 64
- added `PercentPhaseFOV`: 100.0
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 64
- added `EchoTrainLength`: 64
- added `PixelBandwidth`: 2520.0
- added `BandwidthPerPixelPhaseEncode`: 33.96739130434783
- added `DerivedVendorReportedEchoSpacing`: 0.00046
- added `SliceThickness`: 3.0
- added `SpacingBetweenSlices`: 3.6
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TrioTim"
- removed `RepetitionTimeExcitation`: 2.0
- changed `PulseSequenceType`: "EPI" -> "Gradient Echo EPI"
- changed `PhaseEncodingDirection`: "AP" -> "j-"
- changed `EffectiveEchoSpacing`: 6.200396825396825e-06 -> 0.00046
- changed `SliceTiming`: "Interleaved Ascending" -> [0.0, 1.030303, 0.060606, 1.090909, 0.121212, 1.151515, 0.181818, 1.212121, 0.242424, 1.272727, 0.30303, 1.333333, 0.363636, 1.393939, 0.424242, 1.454545, 0.484848, 1.515152, 0.545455, 1.575758, 0.606061, 1.636364, 0.666667, 1.69697, 0.727273, 1.757576, 0.787879, 1.818182, 0.848485, 1.878788, 0.909091, 1.939394, 0.969697]
- changed `SliceEncodingDirection`: "A" -> "k"

## BNU/3/T1w.json

- added `ManufacturersModelName`: "TrioTim"
- added `SoftwareVersions`: "syngo MR B15"
- added `ReceiveCoilActiveElements`: "HEA;HEP"
- added `MatrixCoilMode`: "Auto (CP)"
- added `CoilCombinationMethod`: "Adaptive Combine"
- added `ScanningSequence`: ["GR", "IR"]
- added `SequenceVariant`: ["SK", "SP", "MP"]
- added `ScanOptions`: ["IR"]
- added `SequenceName`: "tfl"
- added `PulseSequenceDetails`: "%SiemensSeq%\\tfl"
- added `SpoilingState`: true
- added `SpoilingType`: "COMBINED"
- added `SpoilingRFPhaseIncrement`: 50.0
- added `B0ShimmingTechnique`: "Tune up"
- added `RepetitionTime`: 2.5300000000000002
- added `RepetitionTimeExcitation`: 0.0078
- added `Modality`: "MR"
- added `ProtocolName`: "t1_mprage_sag"
- added `SeriesDescription`: "t1_mprage_sag"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 256
- added `AcquisitionMatrixPE`: 192
- added `ReconMatrixPE`: 256
- added `PercentPhaseFOV`: 100.0
- added `PercentSampling`: 75.0
- added `PhaseResolution`: 0.75
- added `PhaseEncodingSteps`: 192
- added `PixelBandwidth`: 190.0
- added `SliceThickness`: 1.33
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceTiming`: "Interleaved Ascending"
- changed `SliceEncodingDirection`: "S" -> "i"
- changed `DwellTime`: 2.055921052631579e-05 -> 1.028e-05

## BNU/3/bold.json

- added `ManufacturersModelName`: "TrioTim"
- added `SoftwareVersions`: "syngo MR B15"
- added `ReceiveCoilActiveElements`: "HEA;HEP"
- added `MatrixCoilMode`: "Auto (CP)"
- added `CoilCombinationMethod`: "rSOS"
- added `ScanningSequence`: ["EP"]
- added `SequenceVariant`: ["SK"]
- added `ScanOptions`: ["FS"]
- added `SequenceName`: "ep2d_bold"
- added `PulseSequenceDetails`: "%SiemensSeq%\\ep2d_bold"
- added `MRAcquisitionType`: "2D"
- added `MTState`: false
- added `B0ShimmingTechnique`: "Standard"
- added `NumberShots`: 1
- added `TotalReadoutTime`: 0.028980000000000002
- added `DwellTime`: 3.1000000000000004e-06
- added `RepetitionTime`: 2.0
- added `Modality`: "MR"
- added `ProtocolName`: "ge_func_31x31x35_240_RS"
- added `SeriesDescription`: "ge_func_31x31x35_240_RS"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 64
- added `AcquisitionMatrixPE`: 64
- added `ReconMatrixPE`: 64
- added `PercentPhaseFOV`: 100.0
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 64
- added `EchoTrainLength`: 64
- added `PixelBandwidth`: 2520.0
- added `BandwidthPerPixelPhaseEncode`: 33.96739130434783
- added `DerivedVendorReportedEchoSpacing`: 0.00046
- added `SliceThickness`: 3.5
- added `SpacingBetweenSlices`: 4.2
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TrioTim"
- removed `RepetitionTimeExcitation`: 2.0
- changed `PulseSequenceType`: "EPI" -> "Gradient Echo EPI"
- changed `PhaseEncodingDirection`: "AP" -> "j-"
- changed `EffectiveEchoSpacing`: 6.200396825396825e-06 -> 0.00046
- changed `SliceTiming`: "Interleaved Ascending" -> [0.0, 1.030303, 0.060606, 1.090909, 0.121212, 1.151515, 0.181818, 1.212121, 0.242424, 1.272727, 0.30303, 1.333333, 0.363636, 1.393939, 0.424242, 1.454545, 0.484848, 1.515152, 0.545455, 1.575758, 0.606061, 1.636364, 0.666667, 1.69697, 0.727273, 1.757576, 0.787879, 1.818182, 0.848485, 1.878788, 0.909091, 1.939394, 0.969697]
- changed `SliceEncodingDirection`: "A" -> "k"

## BNU/3/dwi.json

- added `ManufacturersModelName`: "TrioTim"
- added `SoftwareVersions`: "syngo MR B15"
- added `ReceiveCoilActiveElements`: "HEA;HEP"
- added `MatrixCoilMode`: "Auto (Triple)"
- added `CoilCombinationMethod`: "Adaptive Combine"
- added `ScanningSequence`: ["EP"]
- added `SequenceVariant`: ["SK", "SP"]
- added `ScanOptions`: ["FS"]
- added `SequenceName`: "ep2d_diff"
- added `PulseSequenceDetails`: "%SiemensSeq%\\ep2d_diff"
- added `MRAcquisitionType`: "2D"
- added `MTState`: false
- added `B0ShimmingTechnique`: "Standard"
- added `NumberShots`: 1
- added `TotalReadoutTime`: 0.05207
- added `DwellTime`: 2.798e-06
- added `RepetitionTime`: 7.2
- added `Modality`: "MR"
- added `ProtocolName`: "DTI_64_18x18x25_p2_opt"
- added `SeriesDescription`: "DTI_64_18x18x25_p2_opt"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 128
- added `AcquisitionMatrixPE`: 128
- added `ReconMatrixPE`: 128
- added `PercentPhaseFOV`: 100.0
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 128
- added `EchoTrainLength`: 64
- added `PixelBandwidth`: 1396.0
- added `BandwidthPerPixelPhaseEncode`: 19.054878048780488
- added `DerivedVendorReportedEchoSpacing`: 0.00082
- added `SliceThickness`: 2.5
- added `SpacingBetweenSlices`: 2.5
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TrioTim"
- changed `PulseSequenceType`: "EPI" -> "Spin Echo EPI"
- changed `PhaseEncodingDirection`: "AP" -> "j-"
- changed `EffectiveEchoSpacing`: 5.59634670487106e-06 -> 0.00041
- changed `SliceTiming`: "Interleaved Ascending" -> [0.0, 3.673469, 0.146939, 3.820408, 0.293878, 3.967347, 0.440816, 4.114286, 0.587755, 4.261224, 0.734694, 4.408163, 0.881633, 4.555102, 1.028571, 4.702041, 1.17551, 4.84898, 1.322449, 4.995918, 1.469388, 5.142857, 1.616327, 5.289796, 1.763265, 5.436735, 1.910204, 5.583673, 2.057143, 5.730612, 2.204082, 5.877551, 2.35102, 6.02449, 2.497959, 6.171429, 2.644898, 6.318367, 2.791837, 6.465306, 2.938776, 6.612245, 3.085714, 6.759184, 3.232653, 6.906122, 3.379592, 7.053061, 3.526531]
- changed `SliceEncodingDirection`: "a" -> "k"

## DC/1/T1w.json

- added `ReceiveCoilName`: "32 Chan"
- added `RepetitionTime`: 2.375
- added `PixelBandwidth`: 191.4
- added `SliceThickness`: 1.0
- added `AcquisitionMatrixPE`: 187
- removed `ManufacturerModelName`: "N/A"
- removed `PhaseEncodingDirection`: "RL"
- removed `SliceEncodingDirection`: "A"
- removed `RepetitionTimePreparation`: 2.375
- removed `DwellTime`: 2.176941832114246e-05
- changed `PulseSequenceType`: "T1-TFE" -> "3D T1-TFE"

## DC/1/bold.json

- added `ReceiveCoilName`: "32 Chan"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP"]
- added `RepetitionTime`: 2.5
- added `PhilipsReportedBandwidth`: 3625.0
- added `SliceThickness`: 3.5
- added `SpacingBetweenSlices`: 4.0
- added `AcquisitionMatrixPE`: 80
- removed `ManufacturerModelName`: "N/A"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "A"
- removed `RepetitionTimeExcitation`: 2.5
- removed `EffectiveEchoSpacing`: 3.4482758620689654e-06

## HNU/1/T1w.json

- added `ManufacturersModelName`: "Discovery MR750"
- added `ReceiveCoilName`: "8 Chan"
- added `RepetitionTime`: 0.008060000000000001
- added `PixelBandwidth`: 125.0
- added `SliceThickness`: 1.0
- added `AcquisitionMatrixPE`: 250
- removed `ManufacturerModelName`: "Discovery MR750"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "S"
- removed `SliceTiming`: "Interleaved Ascending"
- removed `RepetitionTimePreparation`: 0.008060000000000001
- removed `DwellTime`: 3.2e-05
- changed `PulseSequenceType`: "SPGR" -> "3D SPGR"

## HNU/1/bold.json

- added `ManufacturersModelName`: "Discovery MR750"
- added `ReceiveCoilName`: "8 Chan"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP"]
- added `RepetitionTime`: 2.0
- added `PixelBandwidth`: 3437.5
- added `SliceThickness`: 3.4
- added `SpacingBetweenSlices`: 3.4
- added `AcquisitionMatrixPE`: 64
- removed `ManufacturerModelName`: "Discovery MR750"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "A"
- removed `SliceTiming`: "Interleaved Ascending"
- removed `RepetitionTimeExcitation`: 2.0
- removed `EffectiveEchoSpacing`: 4.5454545454545455e-06

## HNU/1/dwi.json

- added `ManufacturersModelName`: "Discovery MR750"
- added `ReceiveCoilName`: "8 Chan"
- added `RepetitionTime`: 8.6
- added `SliceThickness`: 1.5
- added `AcquisitionMatrixPE`: 128
- removed `ManufacturerModelName`: "Discovery MR750"
- removed `PhaseEncodingDirection`: "RL"
- removed `SliceTiming`: "Interleaved Ascending"

## IACAS/1/T1w.json

- added `ReceiveCoilName`: "8HRBRAIN"
- added `Modality`: "MR"
- added `ProtocolName`: "Sag 3D BRAVO RIGHT"
- added `SeriesDescription`: "Sag 3D BRAVO RIGHT"
- added `ScanningSequence`: ["GR", "IR"]
- added `AcquisitionMatrixPE`: 256
- added `PercentPhaseFOV`: 90.0
- added `PixelBandwidth`: 244.141
- added `NumberOfAverages`: 1.0
- added `SliceThickness`: 1.0
- added `PhaseEncodingAxis`: "j"
- removed `ManufacturerModelName`: "Signa HDx"
- removed `EchoTime`: 0.002984
- removed `PhaseEncodingDirection`: "RL"
- removed `SliceEncodingDirection`: "S"
- removed `SliceTiming`: "Sequential Ascending"
- removed `RepetitionTimePreparation`: 0.007788000000000001
- removed `DwellTime`: 3.201844262295082e-05

## IACAS/1/bold.json

- added `ReceiveCoilName`: "8HRBRAIN"
- added `Modality`: "MR"
- added `ProtocolName`: "BrainWave REST"
- added `SeriesDescription`: "BrainWave REST"
- added `RepetitionTime`: 2.0
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP", "GR"]
- added `AcquisitionMatrixPE`: 64
- added `PercentPhaseFOV`: 100.0
- added `NumberOfAverages`: 1.0
- added `SliceThickness`: 4.0
- added `SpacingBetweenSlices`: 4.6
- added `NumberShots`: 1
- added `PhaseEncodingAxis`: "j"
- removed `ManufacturerModelName`: "Signa HDx"
- removed `PhaseEncodingDirection`: "RL"
- removed `RepetitionTimeExcitation`: 2.0
- removed `EffectiveEchoSpacing`: 2e-06
- changed `PulseSequenceType`: "EPI" -> "Gradient Echo EPI"
- changed `SliceTiming`: "Interleaved Ascending" -> [0.0, 1.0, 0.0625, 1.0625, 0.125, 1.125, 0.1875, 1.1875, 0.25, 1.25, 0.3125, 1.3125, 0.375, 1.375, 0.4375, 1.4375, 0.5, 1.5, 0.5625, 1.5625, 0.625, 1.625, 0.6875, 1.6875, 0.75, 1.75, 0.8125, 1.8125, 0.875, 1.875, 0.9375, 1.9375]

## IBA/TRT/T1w.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "12 Chan"
- added `RepetitionTime`: 2.6
- added `PixelBandwidth`: 130.0
- added `SliceThickness`: 1.0
- added `AcquisitionMatrixPE`: 256
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "S"
- removed `SliceTiming`: "Sequential Ascending"
- removed `RepetitionTimePreparation`: 2.6
- removed `DwellTime`: 3.0048076923076925e-05
- changed `PulseSequenceType`: "MPRAGE" -> "3D MPRAGE"

## IBA/TRT/bold.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "12 Chan"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP"]
- added `RepetitionTime`: 1.75
- added `PixelBandwidth`: 2442.0
- added `SliceThickness`: 3.6
- added `SpacingBetweenSlices`: 3.96
- added `AcquisitionMatrixPE`: 64
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "A"
- removed `SliceTiming`: "Sequential Ascending"
- removed `RepetitionTimeExcitation`: 1.75
- removed `EffectiveEchoSpacing`: 6.398443898443899e-06

## IPCAS/1/T1w.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "8 Chan"
- added `RepetitionTime`: 2.5300000000000002
- added `PixelBandwidth`: 170.0
- added `SliceThickness`: 1.3
- added `SpacingBetweenSlices`: 1.95
- added `AcquisitionMatrixPE`: 256
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "S"
- removed `SliceTiming`: "Sequential Ascending"
- removed `RepetitionTimePreparation`: 2.5300000000000002
- removed `DwellTime`: 2.2977941176470588e-05
- changed `MRAcquisitionType`: "3D" -> "2D"

## IPCAS/1/bold.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "8 Chan"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP"]
- added `RepetitionTime`: 2.0
- added `PixelBandwidth`: 2232.0
- added `SliceThickness`: 4.0
- added `SpacingBetweenSlices`: 4.8
- added `AcquisitionMatrixPE`: 64
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "A"
- removed `RepetitionTimeExcitation`: 2.0
- removed `EffectiveEchoSpacing`: 7.000448028673835e-06
- changed `SliceTiming`: "Interleaved Ascending" -> [1.0, 0.0, 1.0625, 0.0625, 1.125, 0.125, 1.1875, 0.1875, 1.25, 0.25, 1.3125, 0.3125, 1.375, 0.375, 1.4375, 0.4375, 1.5, 0.5, 1.5625, 0.5625, 1.625, 0.625, 1.6875, 0.6875, 1.75, 0.75, 1.8125, 0.8125, 1.875, 0.875, 1.9375, 0.9375]

## IPCAS/1/dwi.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "8 Chan"
- removed `ManufacturerModelName`: "TrioTim"

## IPCAS/2/T1w.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "32 Chan"
- added `RepetitionTime`: 2.3000000000000003
- added `PixelBandwidth`: 130.0
- added `SliceThickness`: 1.2
- added `SpacingBetweenSlices`: 1.8
- added `AcquisitionMatrixPE`: 226
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "S"
- removed `SliceTiming`: "Sequential Ascending"
- removed `RepetitionTimePreparation`: 2.3000000000000003
- removed `DwellTime`: 3.205128205128205e-05
- changed `MRAcquisitionType`: "3D" -> "2D"

## IPCAS/2/bold.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "32 Chan"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP"]
- added `RepetitionTime`: 2.5
- added `PixelBandwidth`: 2232.0
- added `SliceThickness`: 3.0
- added `SpacingBetweenSlices`: 3.99
- added `AcquisitionMatrixPE`: 64
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "A"
- removed `RepetitionTimeExcitation`: 2.5
- removed `EffectiveEchoSpacing`: 7.000448028673835e-06
- changed `SliceTiming`: "Interleaved Ascending" -> [1.25, 0.0, 1.328125, 0.078125, 1.40625, 0.15625, 1.484375, 0.234375, 1.5625, 0.3125, 1.640625, 0.390625, 1.71875, 0.46875, 1.796875, 0.546875, 1.875, 0.625, 1.953125, 0.703125, 2.03125, 0.78125, 2.109375, 0.859375, 2.1875, 0.9375, 2.265625, 1.015625, 2.34375, 1.09375, 2.421875, 1.171875]

## IPCAS/2/dwi.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "32 Chan"
- removed `ManufacturerModelName`: "TrioTim"

## IPCAS/3/T1w.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "8 Chan"
- added `RepetitionTime`: 2.5300000000000002
- added `PixelBandwidth`: 170.0
- added `SliceThickness`: 1.33
- added `AcquisitionMatrixPE`: 256
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "S"
- removed `SliceTiming`: "Interleaved Ascending"
- removed `RepetitionTimePreparation`: 2.5300000000000002
- removed `DwellTime`: 2.2977941176470588e-05
- changed `PulseSequenceType`: "MPRAGE" -> "3D MPRAGE"

## IPCAS/3/bold.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "8 Chan"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP"]
- added `RepetitionTime`: 2.0
- added `PixelBandwidth`: 2232.0
- added `SliceThickness`: 3.0
- added `SpacingBetweenSlices`: 3.99
- added `AcquisitionMatrixPE`: 64
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "A"
- removed `SliceTiming`: "Interleaved Ascending"
- removed `RepetitionTimeExcitation`: 2.0
- removed `EffectiveEchoSpacing`: 7.000448028673835e-06

## IPCAS/4/T1w.json

- added `ManufacturersModelName`: "Discovery"
- added `ReceiveCoilName`: "8 Chan"
- added `RepetitionTime`: 8.068
- added `PixelBandwidth`: 31.25
- added `SliceThickness`: 1.0
- added `AcquisitionMatrixPE`: 250
- removed `ManufacturerModelName`: "Discovery"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "S"
- removed `SliceTiming`: "Interleaved Ascending"
- removed `RepetitionTimePreparation`: 8.068
- removed `DwellTime`: 0.000128
- changed `PulseSequenceType`: "SPGR" -> "3D SPGR"

## IPCAS/4/bold.json

- added `ManufacturersModelName`: "Discovery MR750"
- added `ReceiveCoilName`: "8 Chan"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP"]
- added `RepetitionTime`: 2.0
- added `PixelBandwidth`: 250.0
- added `SliceThickness`: 3.5
- added `SpacingBetweenSlices`: 3.5
- added `AcquisitionMatrixPE`: 64
- removed `ManufacturerModelName`: "Discovery MR750"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "A"
- removed `SliceTiming`: "Interleaved Ascending"
- removed `RepetitionTimeExcitation`: 2.0
- removed `EffectiveEchoSpacing`: 6.25e-05

## IPCAS/5/T1w.json

- added `ManufacturersModelName`: "TrioTim"
- added `SoftwareVersions`: "syngo MR B17"
- added `ReceiveCoilActiveElements`: "HEA;HEP"
- added `MatrixCoilMode`: "Auto (CP)"
- added `CoilCombinationMethod`: "Adaptive Combine"
- added `ScanningSequence`: ["GR", "IR"]
- added `SequenceVariant`: ["SK", "SP", "MP"]
- added `ScanOptions`: ["IR"]
- added `SequenceName`: "tfl"
- added `PulseSequenceDetails`: "%SiemensSeq%\\tfl"
- added `SpoilingState`: true
- added `SpoilingType`: "COMBINED"
- added `SpoilingRFPhaseIncrement`: 50.0
- added `B0ShimmingTechnique`: "Tune up"
- added `RepetitionTime`: 2.5300000000000002
- added `RepetitionTimeExcitation`: 0.0078
- added `Modality`: "MR"
- added `ProtocolName`: "t1_mprage_sag"
- added `SeriesDescription`: "t1_mprage_sag"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 256
- added `AcquisitionMatrixPE`: 192
- added `ReconMatrixPE`: 256
- added `PercentPhaseFOV`: 100.0
- added `PercentSampling`: 75.0
- added `PhaseResolution`: 0.75
- added `PhaseEncodingSteps`: 192
- added `PixelBandwidth`: 190.0
- added `SliceThickness`: 1.33
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TrioTim"
- removed `ParallelReductionFactorInPlane`: 2.0
- removed `ParallelAcquisitionTechnique`: "GRAPPA"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceTiming`: "Interleaved Ascending"
- changed `EchoTime`: 0.0035 -> 0.0033900000000000002
- changed `SliceEncodingDirection`: "S" -> "i"
- changed `DwellTime`: 2.055921052631579e-05 -> 1.028e-05

## IPCAS/5/bold.json

- added `ManufacturersModelName`: "TrioTim"
- added `SoftwareVersions`: "syngo MR B17"
- added `ReceiveCoilActiveElements`: "HEA;HEP"
- added `MatrixCoilMode`: "Auto (CP)"
- added `CoilCombinationMethod`: "rSOS"
- added `ScanningSequence`: ["EP"]
- added `SequenceVariant`: ["SK"]
- added `ScanOptions`: ["FS"]
- added `SequenceName`: "ep2d_bold"
- added `PulseSequenceDetails`: "%SiemensSeq%\\ep2d_bold"
- added `MRAcquisitionType`: "2D"
- added `MTState`: false
- added `B0ShimmingTechnique`: "Standard"
- added `NumberShots`: 1
- added `TotalReadoutTime`: 0.028980000000000002
- added `DwellTime`: 3.1000000000000004e-06
- added `RepetitionTime`: 2.0
- added `Modality`: "MR"
- added `ProtocolName`: "ge_func_31x31x30_300_RS"
- added `SeriesDescription`: "ge_func_31x31x30_300_RS"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 64
- added `AcquisitionMatrixPE`: 64
- added `ReconMatrixPE`: 64
- added `PercentPhaseFOV`: 100.0
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 64
- added `EchoTrainLength`: 64
- added `PixelBandwidth`: 2520.0
- added `BandwidthPerPixelPhaseEncode`: 33.96739130434783
- added `DerivedVendorReportedEchoSpacing`: 0.00046
- added `SliceThickness`: 3.0
- added `SpacingBetweenSlices`: 3.9
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TrioTim"
- removed `RepetitionTimeExcitation`: 2.0
- changed `PulseSequenceType`: "EPI" -> "Gradient Echo EPI"
- changed `PhaseEncodingDirection`: "FH" -> "j-"
- changed `EffectiveEchoSpacing`: 6.799390774586597e-06 -> 0.00046
- changed `SliceTiming`: "Interleaved Ascending" -> [0.0, 1.030303, 0.060606, 1.090909, 0.121212, 1.151515, 0.181818, 1.212121, 0.242424, 1.272727, 0.30303, 1.333333, 0.363636, 1.393939, 0.424242, 1.454545, 0.484848, 1.515152, 0.545455, 1.575758, 0.606061, 1.636364, 0.666667, 1.69697, 0.727273, 1.757576, 0.787879, 1.818182, 0.848485, 1.878788, 0.909091, 1.939394, 0.969697]
- changed `SliceEncodingDirection`: "C" -> "k"

## IPCAS/6/T1w.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "8 Chan"
- added `RepetitionTime`: 1.9000000000000001
- added `PixelBandwidth`: 170.0
- added `SliceThickness`: 1.0
- added `AcquisitionMatrixPE`: 246
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "S"
- removed `SliceTiming`: "Sequential Ascending"
- removed `RepetitionTimePreparation`: 1.9000000000000001
- removed `DwellTime`: 2.2977941176470588e-05
- changed `PulseSequenceType`: "MPRAGE" -> "3D MPRAGE"

## IPCAS/6/bold.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "8 Chan"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP"]
- added `RepetitionTime`: 2.5
- added `PixelBandwidth`: 2298.0
- added `SliceThickness`: 3.5
- added `SpacingBetweenSlices`: 7.0
- added `AcquisitionMatrixPE`: 64
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "A"
- removed `RepetitionTimeExcitation`: 2.5
- removed `EffectiveEchoSpacing`: 6.799390774586597e-06
- changed `SliceTiming`: "Interleaved Ascending" -> [0.0, 1.3, 0.1, 1.4, 0.2, 1.5, 0.3, 1.6, 0.4, 1.7, 0.5, 1.8, 0.6, 1.9, 0.7, 2.0, 0.8, 2.1, 0.9, 2.2, 1.0, 2.3, 1.1, 2.4, 1.2]

## IPCAS/7/T1w.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "8 Chan"
- added `RepetitionTime`: 2.6
- added `PixelBandwidth`: 180.0
- added `SliceThickness`: 1.0
- added `AcquisitionMatrixPE`: 256
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceTiming`: "Sequential Ascending"
- removed `RepetitionTimePreparation`: 2.6
- removed `DwellTime`: 2.170138888888889e-05
- changed `PulseSequenceType`: "MPRAGE" -> "3D MPRAGE"

## IPCAS/7/bold.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "8 Chan"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP"]
- added `RepetitionTime`: 2.5
- added `PixelBandwidth`: 2240.0
- added `SliceThickness`: 3.0
- added `SpacingBetweenSlices`: 3.33
- added `AcquisitionMatrixPE`: 72
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "A"
- removed `RepetitionTimeExcitation`: 2.5
- removed `EffectiveEchoSpacing`: 6.200396825396825e-06
- changed `SliceTiming`: "Interleaved Ascending" -> [1.25, 0.0, 1.315789, 0.065789, 1.381579, 0.131579, 1.447368, 0.197368, 1.513158, 0.263158, 1.578947, 0.328947, 1.644737, 0.394737, 1.710526, 0.460526, 1.776316, 0.526316, 1.842105, 0.592105, 1.907895, 0.657895, 1.973684, 0.723684, 2.039474, 0.789474, 2.105263, 0.855263, 2.171053, 0.921053, 2.236842, 0.986842, 2.302632, 1.052632, 2.368421, 1.118421, 2.434211, 1.184211]

## IPCAS/8/T1w.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "12 Chan"
- added `RepetitionTime`: 2.5300000000000002
- added `PixelBandwidth`: 190.0
- added `SliceThickness`: 1.3
- added `AcquisitionMatrixPE`: 192
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "S"
- removed `SliceTiming`: "Interleaved Ascending"
- removed `RepetitionTimePreparation`: 2.5300000000000002
- removed `DwellTime`: 2.055921052631579e-05
- changed `PulseSequenceType`: "MPRAGE" -> "3D MPRAGE"

## IPCAS/8/bold.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "12 Chan"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP"]
- added `RepetitionTime`: 2.0
- added `PixelBandwidth`: 2520.0
- added `SliceThickness`: 3.0
- added `SpacingBetweenSlices`: 3.9
- added `AcquisitionMatrixPE`: 64
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "A"
- removed `RepetitionTimeExcitation`: 2.0
- removed `EffectiveEchoSpacing`: 6.200396825396825e-06
- changed `SliceTiming`: "Interleaved Ascending" -> [0.0, 1.030303, 0.060606, 1.090909, 0.121212, 1.151515, 0.181818, 1.212121, 0.242424, 1.272727, 0.30303, 1.333333, 0.363636, 1.393939, 0.424242, 1.454545, 0.484848, 1.515152, 0.545455, 1.575758, 0.606061, 1.636364, 0.666667, 1.69697, 0.727273, 1.757576, 0.787879, 1.818182, 0.848485, 1.878788, 0.909091, 1.939394, 0.969697]

## IPCAS/8/dwi.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "12 Chan"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP"]
- added `RepetitionTime`: 6.6000000000000005
- added `PixelBandwidth`: 1396.0
- added `SliceThickness`: 3.0
- added `SpacingBetweenSlices`: 3.0
- added `AcquisitionMatrixPE`: 128
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "a"
- removed `SliceTiming`: "Interleaved Ascending"
- removed `EffectiveEchoSpacing`: 5.59634670487106e-06

## JHNU/1/T1w.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "8 Chan"
- added `RepetitionTime`: 2.3000000000000003
- added `PixelBandwidth`: 240.0
- added `SliceThickness`: 1.0
- added `AcquisitionMatrixPE`: 256
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "S"
- removed `SliceTiming`: "Sequential Ascending"
- removed `RepetitionTimePreparation`: 2.3000000000000003
- removed `DwellTime`: 1.6276041666666666e-05
- changed `PulseSequenceType`: "MPRAGE" -> "3D MPRAGE"

## JHNU/1/bold.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "8 Chan"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP"]
- added `RepetitionTime`: 2.0
- added `PixelBandwidth`: 2230.0
- added `SliceThickness`: 4.0
- added `SpacingBetweenSlices`: 4.4
- added `AcquisitionMatrixPE`: 64
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "A"
- removed `RepetitionTimeExcitation`: 2.0
- removed `EffectiveEchoSpacing`: 7.006726457399103e-06
- changed `SliceTiming`: "Interleaved Ascending" -> [1.0, 0.0, 1.066667, 0.066667, 1.133333, 0.133333, 1.2, 0.2, 1.266667, 0.266667, 1.333333, 0.333333, 1.4, 0.4, 1.466667, 0.466667, 1.533333, 0.533333, 1.6, 0.6, 1.666667, 0.666667, 1.733333, 0.733333, 1.8, 0.8, 1.866667, 0.866667, 1.933333, 0.933333]

## LMU/1/T1w.json

- added `ManufacturersModelName`: "Achieva"
- added `ReceiveCoilName`: "32 Chan"
- added `RepetitionTime`: 2.375
- added `PixelBandwidth`: 191.5
- added `SliceThickness`: 1.0
- added `AcquisitionMatrixPE`: 187
- removed `ManufacturerModelName`: "Achieva"
- removed `ParallelReductionFactorOutOfPlane`: 2.5
- removed `PhaseEncodingDirection`: "RL"
- removed `SliceEncodingDirection`: "A"
- removed `SliceTiming`: "Sequential Ascending"
- removed `RepetitionTimePreparation`: 2.375
- removed `DwellTime`: 2.175805047867711e-05
- changed `PulseSequenceType`: "T1-TFE" -> "3D T1-TFE"

## LMU/1/bold.json

- added `ManufacturersModelName`: "Achieva"
- added `ReceiveCoilName`: "32 Chan"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP"]
- added `RepetitionTime`: 2.5
- added `PhilipsReportedBandwidth`: 2032.0
- added `SliceThickness`: 3.0
- added `SpacingBetweenSlices`: 3.0
- added `AcquisitionMatrixPE`: 79
- removed `ManufacturerModelName`: "Achieva"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "A"
- removed `SliceTiming`: "Sequential Ascending"
- removed `RepetitionTimeExcitation`: 2.5
- removed `EffectiveEchoSpacing`: 6.475341898052217e-06

## LMU/2/T1w.json

- added `ManufacturersModelName`: "Verio"
- added `ReceiveCoilName`: "12 Chan"
- added `RepetitionTime`: 2.4
- added `PixelBandwidth`: 230.0
- added `SliceThickness`: 1.0
- added `AcquisitionMatrixPE`: 246
- removed `ManufacturerModelName`: "Verio"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "S"
- removed `SliceTiming`: "Interleaved Ascending"
- removed `RepetitionTimePreparation`: 2.4
- removed `DwellTime`: 1.6983695652173913e-05
- changed `PulseSequenceType`: "MPRAGE" -> "3D MPRAGE"

## LMU/2/bold.json

- added `ManufacturersModelName`: "Verio"
- added `ReceiveCoilName`: "12 Chan"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP"]
- added `RepetitionTime`: 3.0
- added `PixelBandwidth`: 2232.0
- added `SliceThickness`: 4.0
- added `SpacingBetweenSlices`: 4.4
- added `AcquisitionMatrixPE`: 64
- removed `ManufacturerModelName`: "Verio"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "A"
- removed `SliceTiming`: "Interleaved Ascending"
- removed `RepetitionTimeExcitation`: 3.0
- removed `EffectiveEchoSpacing`: 7.000448028673835e-06

## LMU/3/T1w.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "12 Chan"
- added `RepetitionTime`: 2.4
- added `PixelBandwidth`: 230.0
- added `SliceThickness`: 1.0
- added `AcquisitionMatrixPE`: 246
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "S"
- removed `SliceTiming`: "Interleaved Ascending"
- removed `RepetitionTimePreparation`: 2.4
- removed `DwellTime`: 1.6983695652173913e-05
- changed `PulseSequenceType`: "MPRAGE" -> "3D MPRAGE"

## LMU/3/bold.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "12 Chan"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP"]
- added `RepetitionTime`: 3.0
- added `PixelBandwidth`: 2232.0
- added `SliceThickness`: 4.0
- added `SpacingBetweenSlices`: 4.4
- added `AcquisitionMatrixPE`: 64
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "A"
- removed `SliceTiming`: "Interleaved Ascending"
- removed `RepetitionTimeExcitation`: 3.0
- removed `EffectiveEchoSpacing`: 7.000448028673835e-06

## MPG/1/T1w.json

- added `ManufacturersModelName`: "Magnetom"
- added `ReceiveCoilName`: "24Chan"
- added `RepetitionTime`: 5.0
- added `InversionTime`: 0.9
- added `FlipAngle`: 3.0
- added `PixelBandwidth`: 250.0
- added `PartialFourier`: 0.08
- added `SliceThickness`: 0.7
- added `AcquisitionMatrixPE`: 320
- removed `ManufacturerModelName`: "Magnetom"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "S"
- removed `SliceTiming`: "Interleaved Ascending"
- removed `RepetitionTimePreparation`: 5.0
- removed `DwellTime`: 1.25e-05
- changed `PulseSequenceType`: "MP2RAGE" -> "3D MP2RAGE"

## MPG/1/bold.json

- added `ManufacturersModelName`: "Magentom"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP"]
- added `RepetitionTime`: 3.0
- added `PixelBandwidth`: 1116.0
- added `SliceThickness`: 1.5
- added `SpacingBetweenSlices`: 1.5
- added `AcquisitionMatrixPE`: 128
- removed `ManufacturerModelName`: "Magentom"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "A"
- removed `SliceTiming`: "Interleaved Ascending"
- removed `RepetitionTimeExcitation`: 3.0
- removed `EffectiveEchoSpacing`: 7.000448028673835e-06

## MRN/1/T1w.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "12 Chan"
- added `RepetitionTime`: 2.5300000000000002
- added `EchoTime`: 0.00164
- added `PixelBandwidth`: 651.0
- added `SliceThickness`: 1.0
- added `AcquisitionMatrixPE`: 256
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "S"
- removed `SliceTiming`: "Interleaved Ascending"
- removed `RepetitionTimePreparation`: 2.5300000000000002
- removed `DwellTime`: 6.000384024577573e-06
- changed `PulseSequenceType`: "MEMPR" -> "3D MEMPR"

## MRN/1/bold.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "12 Chan"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP"]
- added `RepetitionTime`: 2.0
- added `PixelBandwidth`: 2170.0
- added `SliceThickness`: 3.5
- added `SpacingBetweenSlices`: 4.55
- added `AcquisitionMatrixPE`: 64
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "A"
- removed `RepetitionTimeExcitation`: 2.0
- removed `EffectiveEchoSpacing`: 7.2004608294930876e-06
- changed `SliceTiming`: "Interleaved Ascending" -> [0.0, 1.030303, 0.060606, 1.090909, 0.121212, 1.151515, 0.181818, 1.212121, 0.242424, 1.272727, 0.30303, 1.333333, 0.363636, 1.393939, 0.424242, 1.454545, 0.484848, 1.515152, 0.545455, 1.575758, 0.606061, 1.636364, 0.666667, 1.69697, 0.727273, 1.757576, 0.787879, 1.818182, 0.848485, 1.878788, 0.909091, 1.939394, 0.969697]

## MRN/1/dwi.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "12 Chan"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP"]
- added `RepetitionTime`: 9.0
- added `PixelBandwidth`: 1562.0
- added `SliceThickness`: 2.0
- added `SpacingBetweenSlices`: 2.0
- added `AcquisitionMatrixPE`: 128
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "a"
- removed `SliceTiming`: "Interleaved Ascending"
- removed `EffectiveEchoSpacing`: 5.001600512163892e-06

## NKI/2/T1w.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "32 Chan"
- added `RepetitionTime`: 1.9000000000000001
- added `PixelBandwidth`: 170.0
- added `SliceThickness`: 1.0
- added `AcquisitionMatrixPE`: 246
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "S"
- removed `SliceTiming`: "Sequential Ascending"
- removed `RepetitionTimePreparation`: 1.9000000000000001
- removed `DwellTime`: 2.2977941176470588e-05
- changed `PulseSequenceType`: "MPRAGE" -> "3D MPRAGE"

## NKI/2/bold_TE=1400.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "32 Chan"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP"]
- added `RepetitionTime`: 1.4000000000000001
- added `PixelBandwidth`: 1786.0
- added `SliceThickness`: 2.0
- added `SpacingBetweenSlices`: 2.0
- added `AcquisitionMatrixPE`: 112
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "A"
- removed `RepetitionTimeExcitation`: 1.4000000000000001
- removed `EffectiveEchoSpacing`: 4.999200127979523e-06
- changed `SliceTiming`: "Interleaved Ascending" -> [0.7, 0.0, 0.721875, 0.021875, 0.74375, 0.04375, 0.765625, 0.065625, 0.7875, 0.0875, 0.809375, 0.109375, 0.83125, 0.13125, 0.853125, 0.153125, 0.875, 0.175, 0.896875, 0.196875, 0.91875, 0.21875, 0.940625, 0.240625, 0.9625, 0.2625, 0.984375, 0.284375, 1.00625, 0.30625, 1.028125, 0.328125, 1.05, 0.35, 1.071875, 0.371875, 1.09375, 0.39375, 1.115625, 0.415625, 1.1375, 0.4375, 1.159375, 0.459375, 1.18125, 0.48125, 1.203125, 0.503125, 1.225, 0.525, 1.246875, 0.546875, 1.26875, 0.56875, 1.290625, 0.590625, 1.3125, 0.6125, 1.334375, 0.634375, 1.35625, 0.65625, 1.378125, 0.678125]

## NKI/2/bold_TE=2500.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "32 Chan"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP"]
- added `RepetitionTime`: 2.5
- added `PixelBandwidth`: 2240.0
- added `SliceThickness`: 3.0
- added `SpacingBetweenSlices`: 3.33
- added `AcquisitionMatrixPE`: 72
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "A"
- removed `SliceTiming`: "Interleaved Ascending"
- removed `RepetitionTimeExcitation`: 2.5
- removed `EffectiveEchoSpacing`: 6.200396825396825e-06

## NKI/2/bold_TE=645.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "32 Chan"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP"]
- added `RepetitionTime`: 0.645
- added `PixelBandwidth`: 2598.0
- added `SliceThickness`: 3.0
- added `SpacingBetweenSlices`: 3.0
- added `AcquisitionMatrixPE`: 74
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "A"
- removed `SliceTiming`: "Interleaved Ascending"
- removed `RepetitionTimeExcitation`: 0.645
- removed `EffectiveEchoSpacing`: 5.201506356240767e-06

## NKI/2/dwi.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "32 Chan"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP"]
- added `RepetitionTime`: 2.4
- added `PixelBandwidth`: 1814.0
- added `SliceThickness`: 2.0
- added `SpacingBetweenSlices`: 2.0
- added `AcquisitionMatrixPE`: 106
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "a"
- removed `SliceTiming`: "Interleaved Ascending"
- removed `EffectiveEchoSpacing`: 5.200640718936573e-06

## NYU/1/T1w.json

- added `ManufacturersModelName`: "Allegra"
- added `ReceiveCoilName`: "1 Chan"
- added `RepetitionTime`: 2.5
- added `SliceThickness`: 1.0
- added `AcquisitionMatrixPE`: 256
- removed `ManufacturerModelName`: "Allegra"
- removed `RepetitionTimePreparation`: 2.5
- changed `MRAcquisitionType`: "3D" -> "2D"

## NYU/1/bold.json

- added `ManufacturersModelName`: "Allegra"
- added `ReceiveCoilName`: "1 Chan"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP"]
- added `RepetitionTime`: 2.0
- added `SliceThickness`: 3.0
- added `AcquisitionMatrixPE`: 64
- removed `ManufacturerModelName`: "Allegra"
- removed `RepetitionTimeExcitation`: 2.0

## NYU/2/T1w.json

- added `ManufacturersModelName`: "Allegra"
- added `SoftwareVersions`: "syngo MR 2004A"
- added `ReceiveCoilActiveElements`: "HE"
- added `ScanningSequence`: ["GR", "IR"]
- added `SequenceVariant`: ["SK", "SP", "MP"]
- added `ScanOptions`: ["IR"]
- added `SequenceName`: "tfl"
- added `PulseSequenceDetails`: "%SiemensSeq%\\tfl"
- added `SpoilingState`: true
- added `SpoilingType`: "COMBINED"
- added `SpoilingRFPhaseIncrement`: 50.0
- added `B0ShimmingTechnique`: "Standard"
- added `RepetitionTime`: 2.5300000000000002
- added `RepetitionTimeExcitation`: 0.0074
- added `Modality`: "MR"
- added `ProtocolName`: "HighResT1"
- added `SeriesDescription`: "HighResT1"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 256
- added `AcquisitionMatrixPE`: 192
- added `ReconMatrixPE`: 256
- added `PercentPhaseFOV`: 100.0
- added `PercentSampling`: 75.0
- added `PhaseResolution`: 0.75
- added `PhaseEncodingSteps`: 192
- added `PixelBandwidth`: 200.0
- added `SliceThickness`: 1.33
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "Allegra"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceTiming`: "Sequential Ascending"
- changed `SliceEncodingDirection`: "S" -> "i"
- changed `DwellTime`: 1.953125e-05 -> 9.766000000000001e-06

## NYU/2/bold.json

- added `ManufacturersModelName`: "Allegra"
- added `SoftwareVersions`: "syngo MR 2004A"
- added `ReceiveCoilActiveElements`: "TR"
- added `ScanningSequence`: ["EP"]
- added `SequenceVariant`: ["SK"]
- added `ScanOptions`: ["FS"]
- added `SequenceName`: "cbi_seg_epi_gre"
- added `PulseSequenceDetails`: "%CustomerSeq%\\cbi_seg_epi_gre"
- added `MRAcquisitionType`: "2D"
- added `MTState`: false
- added `B0ShimmingTechnique`: "Standard"
- added `NumberShots`: 1
- added `TotalReadoutTime`: 0.02016
- added `DwellTime`: 1.6000000000000001e-06
- added `RepetitionTime`: 2.0
- added `Modality`: "MR"
- added `ProtocolName`: "Rest_TR2_4mm"
- added `SeriesDescription`: "Rest_TR2_4mm"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 80
- added `AcquisitionMatrixPE`: 64
- added `ReconMatrixPE`: 64
- added `PercentPhaseFOV`: 80.0
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 64
- added `EchoTrainLength`: 64
- added `PixelBandwidth`: 3906.0
- added `BandwidthPerPixelPhaseEncode`: 48.82812499999999
- added `DerivedVendorReportedEchoSpacing`: 0.00032
- added `SliceThickness`: 4.0
- added `SpacingBetweenSlices`: 4.0
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "Allegra"
- removed `RepetitionTimeExcitation`: 2.0
- changed `PulseSequenceType`: "EPI" -> "Gradient Echo EPI"
- changed `PhaseEncodingDirection`: "RL" -> "j"
- changed `EffectiveEchoSpacing`: 3.200204813108039e-06 -> 0.00032
- changed `SliceTiming`: "Interleaved Ascending" -> [0.0, 1.030303, 0.060606, 1.090909, 0.121212, 1.151515, 0.181818, 1.212121, 0.242424, 1.272727, 0.30303, 1.333333, 0.363636, 1.393939, 0.424242, 1.454545, 0.484848, 1.515152, 0.545455, 1.575758, 0.606061, 1.636364, 0.666667, 1.69697, 0.727273, 1.757576, 0.787879, 1.818182, 0.848485, 1.878788, 0.909091, 1.939394, 0.969697]
- changed `SliceEncodingDirection`: "A" -> "k"

## SWU/1/T1w.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "8 Chan"
- added `RepetitionTime`: 1.9000000000000001
- added `PixelBandwidth`: 170.0
- added `SliceThickness`: 1.0
- added `AcquisitionMatrixPE`: 246
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "S"
- removed `SliceTiming`: "Sequential Ascending"
- removed `RepetitionTimePreparation`: 1.9000000000000001
- removed `DwellTime`: 2.2977941176470588e-05
- changed `PulseSequenceType`: "MPRAGE" -> "3D MPRAGE"

## SWU/1/bold.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "8 Chan"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP"]
- added `RepetitionTime`: 2.0
- added `PixelBandwidth`: 2232.0
- added `SliceThickness`: 3.0
- added `SpacingBetweenSlices`: 3.6
- added `AcquisitionMatrixPE`: 64
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "A"
- removed `RepetitionTimeExcitation`: 2.0
- removed `EffectiveEchoSpacing`: 7.000448028673835e-06
- changed `SliceTiming`: "Interleaved Ascending" -> [0.0, 1.030303, 0.060606, 1.090909, 0.121212, 1.151515, 0.181818, 1.212121, 0.242424, 1.272727, 0.30303, 1.333333, 0.363636, 1.393939, 0.424242, 1.454545, 0.484848, 1.515152, 0.545455, 1.575758, 0.606061, 1.636364, 0.666667, 1.69697, 0.727273, 1.757576, 0.787879, 1.818182, 0.848485, 1.878788, 0.909091, 1.939394, 0.969697]

## SWU/2/T1w.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "8 Chan"
- added `RepetitionTime`: 1.9000000000000001
- added `PixelBandwidth`: 170.0
- added `SliceThickness`: 1.0
- added `AcquisitionMatrixPE`: 246
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "S"
- removed `SliceTiming`: "Sequential Ascending"
- removed `RepetitionTimePreparation`: 1.9000000000000001
- removed `DwellTime`: 2.2977941176470588e-05
- changed `PulseSequenceType`: "MPRAGE" -> "3D MPRAGE"

## SWU/2/bold.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "8 Chan"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP"]
- added `RepetitionTime`: 2.0
- added `PixelBandwidth`: 2232.0
- added `SliceThickness`: 3.0
- added `SpacingBetweenSlices`: 3.99
- added `AcquisitionMatrixPE`: 64
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "A"
- removed `RepetitionTimeExcitation`: 2.0
- removed `EffectiveEchoSpacing`: 7.000448028673835e-06
- changed `SliceTiming`: "Interleaved Ascending" -> [1.0, 0.0, 1.0625, 0.0625, 1.125, 0.125, 1.1875, 0.1875, 1.25, 0.25, 1.3125, 0.3125, 1.375, 0.375, 1.4375, 0.4375, 1.5, 0.5, 1.5625, 0.5625, 1.625, 0.625, 1.6875, 0.6875, 1.75, 0.75, 1.8125, 0.8125, 1.875, 0.875, 1.9375, 0.9375]

## SWU/3/T1w.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "8 Chan"
- added `RepetitionTime`: 1.9000000000000001
- added `PixelBandwidth`: 170.0
- added `SliceThickness`: 1.0
- added `AcquisitionMatrixPE`: 246
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "S"
- removed `SliceTiming`: "Sequential Ascending"
- removed `RepetitionTimePreparation`: 1.9000000000000001
- removed `DwellTime`: 2.2977941176470588e-05
- changed `PulseSequenceType`: "MPRAGE" -> "3D MPRAGE"

## SWU/3/bold.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "8 Chan"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP"]
- added `RepetitionTime`: 2.0
- added `PixelBandwidth`: 2232.0
- added `SliceThickness`: 3.0
- added `SpacingBetweenSlices`: 3.99
- added `AcquisitionMatrixPE`: 64
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "A"
- removed `RepetitionTimeExcitation`: 2.0
- removed `EffectiveEchoSpacing`: 7.000448028673835e-06
- changed `SliceTiming`: "Interleaved Ascending" -> [1.0, 0.0, 1.0625, 0.0625, 1.125, 0.125, 1.1875, 0.1875, 1.25, 0.25, 1.3125, 0.3125, 1.375, 0.375, 1.4375, 0.4375, 1.5, 0.5, 1.5625, 0.5625, 1.625, 0.625, 1.6875, 0.6875, 1.75, 0.75, 1.8125, 0.8125, 1.875, 0.875, 1.9375, 0.9375]

## SWU/4/T1w.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "8 Chan"
- added `RepetitionTime`: 1.9000000000000001
- added `PixelBandwidth`: 170.0
- added `SliceThickness`: 1.0
- added `AcquisitionMatrixPE`: 256
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "S"
- removed `SliceTiming`: "Sequential Ascending"
- removed `RepetitionTimePreparation`: 1.9000000000000001
- removed `DwellTime`: 2.2977941176470588e-05
- changed `PulseSequenceType`: "MPRAGE" -> "3D MPRAGE"

## SWU/4/bold.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "8 Chan"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP"]
- added `RepetitionTime`: 2.0
- added `PixelBandwidth`: 2232.0
- added `SliceThickness`: 3.0
- added `SpacingBetweenSlices`: 4.0
- added `AcquisitionMatrixPE`: 64
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "A"
- removed `RepetitionTimeExcitation`: 2.0
- removed `EffectiveEchoSpacing`: 7.000448028673835e-06
- changed `SliceTiming`: "Interleaved Ascending" -> [1.0, 0.0, 1.0625, 0.0625, 1.125, 0.125, 1.1875, 0.1875, 1.25, 0.25, 1.3125, 0.3125, 1.375, 0.375, 1.4375, 0.4375, 1.5, 0.5, 1.5625, 0.5625, 1.625, 0.625, 1.6875, 0.6875, 1.75, 0.75, 1.8125, 0.8125, 1.875, 0.875, 1.9375, 0.9375]

## SWU/4/dwi.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "8 Chan"
- removed `ManufacturerModelName`: "TrioTim"

## UM/1/T1w.json

- added `ManufacturersModelName`: "TrioTim"
- added `SoftwareVersions`: "syngo MR B17"
- added `ReceiveCoilActiveElements`: "HEA;HEP"
- added `MatrixCoilMode`: "Auto (Triple)"
- added `CoilCombinationMethod`: "rSOS"
- added `ScanningSequence`: ["GR", "IR"]
- added `SequenceVariant`: ["SK", "SP", "MP"]
- added `ScanOptions`: ["IR"]
- added `SequenceName`: "tfl"
- added `PulseSequenceDetails`: "%SiemensSeq%\\tfl"
- added `SpoilingState`: true
- added `SpoilingType`: "COMBINED"
- added `SpoilingRFPhaseIncrement`: 50.0
- added `B0ShimmingTechnique`: "Standard"
- added `ParallelReductionFactorOutOfPlane`: 1
- added `RepetitionTime`: 2.3000000000000003
- added `RepetitionTimeExcitation`: 0.0070999999999999995
- added `Modality`: "MR"
- added `ProtocolName`: "MPRAGE ADNI_iPAT2"
- added `SeriesDescription`: "MPRAGE ADNI_iPAT2"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 256
- added `AcquisitionMatrixPE`: 240
- added `ReconMatrixPE`: 240
- added `PercentPhaseFOV`: 93.8
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 240
- added `PixelBandwidth`: 240.0
- added `SliceThickness`: 1.0
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceTiming`: "Interleaved Ascending"
- changed `SliceEncodingDirection`: "S" -> "i-"
- changed `DwellTime`: 1.6276041666666666e-05 -> 8.138e-06

## UM/1/bold.json

- added `ManufacturersModelName`: "TrioTim"
- added `SoftwareVersions`: "syngo MR B17"
- added `ReceiveCoilActiveElements`: "HEA;HEP"
- added `MatrixCoilMode`: "Triple"
- added `CoilCombinationMethod`: "rSOS"
- added `ScanningSequence`: ["EP"]
- added `SequenceVariant`: ["SK"]
- added `ScanOptions`: ["FS"]
- added `SequenceName`: "ep2d_pace"
- added `PulseSequenceDetails`: "%SiemensSeq%\\ep2d_pace"
- added `MRAcquisitionType`: "2D"
- added `MTState`: false
- added `B0ShimmingTechnique`: "Standard"
- added `NumberShots`: 1
- added `TotalReadoutTime`: 0.029609999999999997
- added `DwellTime`: 3.199e-06
- added `RepetitionTime`: 2.0
- added `Modality`: "MR"
- added `ProtocolName`: "RSN1(ep2d_64)"
- added `SeriesDescription`: "RSN1(ep2d_64)"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 64
- added `AcquisitionMatrixPE`: 64
- added `ReconMatrixPE`: 64
- added `PercentPhaseFOV`: 100.0
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 64
- added `EchoTrainLength`: 64
- added `PixelBandwidth`: 2442.0
- added `BandwidthPerPixelPhaseEncode`: 33.24468085106383
- added `DerivedVendorReportedEchoSpacing`: 0.00047
- added `SliceThickness`: 4.0
- added `SpacingBetweenSlices`: 4.0
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TrioTim"
- removed `RepetitionTimeExcitation`: 2.0
- changed `PulseSequenceType`: "EPI" -> "Gradient Echo EPI"
- changed `PhaseEncodingDirection`: "AP" -> "j-"
- changed `EffectiveEchoSpacing`: 6.398443898443899e-06 -> 0.00047
- changed `SliceTiming`: "Sequential Descending" -> [1.889062, 1.828125, 1.767187, 1.70625, 1.645312, 1.584375, 1.523438, 1.4625, 1.401563, 1.340625, 1.279687, 1.21875, 1.157812, 1.096875, 1.035937, 0.975, 0.914062, 0.853125, 0.792187, 0.73125, 0.670312, 0.609375, 0.548438, 0.4875, 0.426563, 0.365625, 0.304688, 0.24375, 0.182812, 0.121875, 0.060937, 0.0]
- changed `SliceEncodingDirection`: "A" -> "k"

## UPSM/1/T1w.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "12 Chan"
- added `RepetitionTime`: 2.1
- added `PixelBandwidth`: 240.0
- added `SliceThickness`: 1.0
- added `AcquisitionMatrixPE`: 256
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "RL"
- removed `SliceEncodingDirection`: "A"
- removed `SliceTiming`: "Interleaved Ascending"
- removed `RepetitionTimePreparation`: 2.1
- removed `DwellTime`: 1.6276041666666666e-05
- changed `PulseSequenceType`: "MPRAGE" -> "3D MPRAGE"

## UPSM/1/bold.json

- added `ManufacturersModelName`: "TrioTim"
- added `ReceiveCoilName`: "12 Chan"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP"]
- added `RepetitionTime`: 1.5
- added `PixelBandwidth`: 2694.0
- added `SliceThickness`: 4.0
- added `SpacingBetweenSlices`: 4.0
- added `AcquisitionMatrixPE`: 64
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "PA"
- removed `SliceEncodingDirection`: "A"
- removed `SliceTiming`: "Sequential Ascending"
- removed `RepetitionTimeExcitation`: 1.5
- removed `EffectiveEchoSpacing`: 5.79992576095026e-06

## Utah/1/T1w.json

- added `ManufacturersModelName`: "TrioTim"
- added `SoftwareVersions`: "syngo MR B17"
- added `ReceiveCoilActiveElements`: "HEA;HEP"
- added `MatrixCoilMode`: "Auto (CP)"
- added `CoilCombinationMethod`: "rSOS"
- added `ScanningSequence`: ["GR", "IR"]
- added `SequenceVariant`: ["SK", "SP", "MP"]
- added `ScanOptions`: ["IR"]
- added `SequenceName`: "tfl"
- added `PulseSequenceDetails`: "%SiemensSeq%\\tfl"
- added `SpoilingState`: true
- added `SpoilingType`: "COMBINED"
- added `SpoilingRFPhaseIncrement`: 50.0
- added `B0ShimmingTechnique`: "Standard"
- added `RepetitionTime`: 2.3000000000000003
- added `RepetitionTimeExcitation`: 0.0068
- added `Modality`: "MR"
- added `ProtocolName`: "SAG MPRAGE"
- added `SeriesDescription`: "SAG MPRAGE"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 256
- added `AcquisitionMatrixPE`: 240
- added `ReconMatrixPE`: 480
- added `PercentPhaseFOV`: 93.8
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 240
- added `PixelBandwidth`: 240.0
- added `SliceThickness`: 1.2
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceTiming`: "Interleaved Ascending"
- changed `SliceEncodingDirection`: "S" -> "i"
- changed `DwellTime`: 1.6276041666666666e-05 -> 8.138e-06

## Utah/1/bold.json

- added `ManufacturersModelName`: "TrioTim"
- added `SoftwareVersions`: "syngo MR B17"
- added `ReceiveCoilActiveElements`: "HEA;HEP"
- added `MatrixCoilMode`: "Auto (Triple)"
- added `CoilCombinationMethod`: "rSOS"
- added `ScanningSequence`: ["EP"]
- added `SequenceVariant`: ["SK"]
- added `ScanOptions`: ["FS"]
- added `SequenceName`: "ep2d_bold_PMU"
- added `PulseSequenceDetails`: "%CustomerSeq%\\ep2d_bold_PMU"
- added `MRAcquisitionType`: "2D"
- added `MTState`: false
- added `B0ShimmingTechnique`: "Standard"
- added `NumberShots`: 1
- added `ParallelReductionFactorInPlane`: 2
- added `ParallelAcquisitionTechnique`: "GRAPPA"
- added `TotalReadoutTime`: 0.013545
- added `DwellTime`: 2.7e-06
- added `RepetitionTime`: 2.0
- added `Modality`: "MR"
- added `ProtocolName`: "ep2d_bold_PMU_resting1"
- added `SeriesDescription`: "ep2d_bold_PMU_resting1"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 64
- added `AcquisitionMatrixPE`: 64
- added `ReconMatrixPE`: 64
- added `PercentPhaseFOV`: 100.0
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 64
- added `EchoTrainLength`: 32
- added `PixelBandwidth`: 2894.0
- added `BandwidthPerPixelPhaseEncode`: 72.67441860465117
- added `DerivedVendorReportedEchoSpacing`: 0.00043
- added `SliceThickness`: 3.0
- added `SpacingBetweenSlices`: 3.3
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TrioTim"
- removed `RepetitionTimeExcitation`: 2.0
- changed `PulseSequenceType`: "EPI" -> "Gradient Echo EPI"
- changed `PhaseEncodingDirection`: "AP" -> "j-"
- changed `EffectiveEchoSpacing`: 5.399101589495508e-06 -> 0.000215
- changed `SliceTiming`: "Interleaved Ascending" -> [1.0, 0.0, 1.05, 0.05, 1.1, 0.1, 1.15, 0.15, 1.2, 0.2, 1.25, 0.25, 1.3, 0.3, 1.35, 0.35, 1.4, 0.4, 1.45, 0.45, 1.5, 0.5, 1.55, 0.55, 1.6, 0.6, 1.65, 0.65, 1.7, 0.7, 1.75, 0.75, 1.8, 0.8, 1.85, 0.85, 1.9, 0.9, 1.95, 0.95]
- changed `SliceEncodingDirection`: "A" -> "k"

## Utah/2/T1w.json

- added `ManufacturersModelName`: "TrioTim"
- added `SoftwareVersions`: "syngo MR B17"
- added `ReceiveCoilActiveElements`: "HEA;HEP"
- added `MatrixCoilMode`: "Auto (CP)"
- added `CoilCombinationMethod`: "rSOS"
- added `ScanningSequence`: ["GR", "IR"]
- added `SequenceVariant`: ["SK", "SP", "MP"]
- added `ScanOptions`: ["IR"]
- added `SequenceName`: "tfl"
- added `PulseSequenceDetails`: "%SiemensSeq%\\tfl"
- added `SpoilingState`: true
- added `SpoilingType`: "COMBINED"
- added `SpoilingRFPhaseIncrement`: 50.0
- added `B0ShimmingTechnique`: "Standard"
- added `RepetitionTime`: 2.3000000000000003
- added `RepetitionTimeExcitation`: 0.0068
- added `Modality`: "MR"
- added `ProtocolName`: "SAG MPRAGE"
- added `SeriesDescription`: "SAG MPRAGE"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 256
- added `AcquisitionMatrixPE`: 240
- added `ReconMatrixPE`: 256
- added `PercentPhaseFOV`: 93.8
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 240
- added `PixelBandwidth`: 240.0
- added `SliceThickness`: 1.2
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TrioTim"
- removed `PhaseEncodingDirection`: "AP"
- removed `SliceTiming`: "Interleaved Ascending"
- changed `SliceEncodingDirection`: "S" -> "i"
- changed `DwellTime`: 1.6276041666666666e-05 -> 8.138e-06

## Utah/2/bold.json

- added `ManufacturersModelName`: "TrioTim"
- added `SoftwareVersions`: "syngo MR B17"
- added `ReceiveCoilActiveElements`: "HEA;HEP"
- added `MatrixCoilMode`: "Auto (Triple)"
- added `CoilCombinationMethod`: "rSOS"
- added `ScanningSequence`: ["EP"]
- added `SequenceVariant`: ["SK"]
- added `ScanOptions`: ["FS"]
- added `SequenceName`: "ep2d_bold_PMU"
- added `PulseSequenceDetails`: "%CustomerSeq%\\ep2d_bold_PMU"
- added `MRAcquisitionType`: "2D"
- added `MTState`: false
- added `B0ShimmingTechnique`: "Standard"
- added `NumberShots`: 1
- added `ParallelReductionFactorInPlane`: 2
- added `ParallelAcquisitionTechnique`: "GRAPPA"
- added `TotalReadoutTime`: 0.013545
- added `DwellTime`: 2.7e-06
- added `RepetitionTime`: 2.0
- added `Modality`: "MR"
- added `ProtocolName`: "ep2d_bold_PMU_resting1"
- added `SeriesDescription`: "ep2d_bold_PMU_resting1"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 64
- added `AcquisitionMatrixPE`: 64
- added `ReconMatrixPE`: 64
- added `PercentPhaseFOV`: 100.0
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 64
- added `EchoTrainLength`: 32
- added `PixelBandwidth`: 2894.0
- added `BandwidthPerPixelPhaseEncode`: 72.67441860465117
- added `DerivedVendorReportedEchoSpacing`: 0.00043
- added `SliceThickness`: 3.0
- added `SpacingBetweenSlices`: 3.3
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TrioTim"
- removed `RepetitionTimeExcitation`: 2.0
- changed `PulseSequenceType`: "EPI" -> "Gradient Echo EPI"
- changed `PhaseEncodingDirection`: "AP" -> "j-"
- changed `EffectiveEchoSpacing`: 5.399101589495508e-06 -> 0.000215
- changed `SliceTiming`: "Interleaved Ascending" -> [1.0, 0.0, 1.05, 0.05, 1.1, 0.1, 1.15, 0.15, 1.2, 0.2, 1.25, 0.25, 1.3, 0.3, 1.35, 0.35, 1.4, 0.4, 1.45, 0.45, 1.5, 0.5, 1.55, 0.55, 1.6, 0.6, 1.65, 0.65, 1.7, 0.7, 1.75, 0.75, 1.8, 0.8, 1.85, 0.85, 1.9, 0.9, 1.95, 0.95]
- changed `SliceEncodingDirection`: "A" -> "k"
