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
