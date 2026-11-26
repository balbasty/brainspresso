## Suspected protocol2bids problems

Collected automatically by comparing the regenerated sidecars with the
previous (hand-parsed) templates. Values were NOT hand-fixed: fix
protocol2bids (or add `defaults`/`assigns` in `sidecars.toml`) and re-run.

### `ManufacturerModelName` dropped (39)

- `BNI/1/T1w.json` <- `BNI_1/anat.txt`: was "Ingenia"
- `BNI/1/bold.json` <- `BNI_1/rest.txt`: was "Ingenia"
- `EMC/1/T1w.json` <- `EMC_1/anat.pdf`: was "MR750"
- `EMC/1/bold.json` <- `EMC_1/rest.pdf`: was "MR750"
- `ETH/1/T1w.json` <- `ETH_1/anat.txt`: was "Achieva"
- `ETH/1/bold.json` <- `ETH_1/rest.txt`: was "Achieva"
- `GU/1/T1w.json` <- `GU_1/T1w.pdf`: was "TriTim"
- `GU/1/bold.json` <- `GU_1/bold.pdf`: was "TriTim"
- `IP/1/T1w.json` <- `IP_1/T1w.pdf`: was "Achieva"
- `IP/1/bold.json` <- `IP_1/bold.pdf`: was "Achieva"
- `IP/1/dwi.json` <- `IP_1/dwi.pdf`: was "Achieva"
- `IU/1/T1w.json` <- `IU_1/T1w.pdf`: was "TriTim"
- `IU/1/bold.json` <- `IU_1/bold.pdf`: was "TriTim"
- `KUL/3/T1w.json` <- `KUL_3/T1w.txt`: was "Achieva Ds"
- `KUL/3/bold.json` <- `KUL_3/bold.txt`: was "Achieva Ds"
- `NYU/1/T1w.json` <- `NYU_1/T1w.pdf`: was "Allegra"
- `NYU/1/bold.json` <- `NYU_1/bold.pdf`: was "Allegra"
- `NYU/1/dwi.json` <- `NYU_1/dwi.pdf`: was "Allegra"
- `NYU/2/T1w.json` <- `NYU_2/T1w.pdf`: was "Allegra"
- `NYU/2/bold.json` <- `NYU_2/bold.pdf`: was "Allegra"
- `NYU/2/dwi.json` <- `NYU_2/dwi.pdf`: was "Allegra"
- `OHSU/1/T1w.json` <- `OHSU_1/T1w.pdf`: was "TriTim"
- `OHSU/1/bold.json` <- `OHSU_1/bold.pdf` (select `REST1`): was "TriTim"
- `SDSU/1/T1w.json` <- `SDSU_1/T1w.pdf`: was "MR750"
- `SDSU/1/bold.json` <- `SDSU_1/bold.pdf`: was "MR750"
- `SDSU/1/dwi.json` <- `SDSU_1/dwi.pdf`: was "MR750"
- `TCD/1/T1w.json` <- `TCD_1/T1w.txt`: was "Achieva"
- `TCD/1/bold.json` <- `TCD_1/bold.txt`: was "Achieva"
- `TCD/1/dwi.json` <- `TCD_1/dwi.txt`: was "Achieva"
- `UCD/1/T1w.json` <- `UCD_1/T1w.pdf`: was "TriTim"
- `UCD/1/bold.json` <- `UCD_1/bold.pdf`: was "TriTim"
- `UCLA/1/T1w.json` <- `UCLA_1/T1w.pdf`: was "TriTim"
- `UCLA/1/bold.json` <- `UCLA_1/bold.pdf`: was "TriTim"
- `UCLA/Long/T1w.json` <- `UCLA_Long/T1w.pdf`: was "TriTim"
- `UCLA/Long/bold.json` <- `UCLA_Long/bold.pdf`: was "TriTim"
- `UPSM/Long/T1w.json` <- `UPSM_Long/T1w.pdf`: was "Allegra"
- `UPSM/Long/bold.json` <- `UPSM_Long/bold.pdf`: was "Allegra"
- `USM/1/T1w.json` <- `USM_1/T1w.pdf`: was "TriTim"
- `USM/1/bold.json` <- `USM_1/bold.pdf`: was "TriTim"

### `RepetitionTimeExcitation` dropped (24)

- `BNI/1/bold.json` <- `BNI_1/rest.txt`: was 3.0
- `BNI/1/dwi.json` <- `BNI_1/dti.txt`: was 0.101
- `EMC/1/T1w.json` <- `EMC_1/anat.pdf`: was 0.01026
- `EMC/1/bold.json` <- `EMC_1/rest.pdf`: was 2.0
- `ETH/1/bold.json` <- `ETH_1/rest.txt`: was 2.0
- `GU/1/bold.json` <- `GU_1/bold.pdf`: was 2.0
- `IP/1/bold.json` <- `IP_1/bold.pdf`: was 2.7
- `IP/1/dwi.json` <- `IP_1/dwi.pdf`: was 5.407
- `IU/1/bold.json` <- `IU_1/bold.pdf`: was 0.8130000000000001
- `KUL/3/bold.json` <- `KUL_3/bold.txt`: was 2.5
- `NYU/1/bold.json` <- `NYU_1/bold.pdf`: was 2.0
- `NYU/1/dwi.json` <- `NYU_1/dwi.pdf`: was 5.2
- `NYU/2/bold.json` <- `NYU_2/bold.pdf`: was 2.0
- `NYU/2/dwi.json` <- `NYU_2/dwi.pdf`: was 5.2
- `OHSU/1/bold.json` <- `OHSU_1/bold.pdf` (select `REST1`): was 2.5
- `SDSU/1/bold.json` <- `SDSU_1/bold.pdf`: was 2.0
- `SDSU/1/dwi.json` <- `SDSU_1/dwi.pdf`: was 8.5
- `TCD/1/bold.json` <- `TCD_1/bold.txt`: was 2.0
- `TCD/1/dwi.json` <- `TCD_1/dwi.txt`: was 20.244
- `UCD/1/bold.json` <- `UCD_1/bold.pdf`: was 2.0
- `UCLA/1/bold.json` <- `UCLA_1/bold.pdf`: was 3.0
- `UCLA/Long/bold.json` <- `UCLA_Long/bold.pdf`: was 3.0
- `UPSM/Long/bold.json` <- `UPSM_Long/bold.pdf`: was 1.5
- `USM/1/bold.json` <- `USM_1/bold.pdf`: was 2.0

### `EffectiveEchoSpacing` differs (12)

- `GU/1/bold.json` <- `GU_1/bold.pdf`: 6.39844e-06 -> 0.000245 (x38.29)
- `IU/1/bold.json` <- `IU_1/bold.pdf`: 6.00038e-06 -> 0.00049 (x81.66)
- `NYU/1/bold.json` <- `NYU_1/bold.pdf`: 3.2002e-06 -> 0.00032 (x99.99)
- `NYU/1/dwi.json` <- `NYU_1/dwi.pdf`: 4.20027e-06 -> 0.00032 (x76.19)
- `NYU/2/bold.json` <- `NYU_2/bold.pdf`: 3.2002e-06 -> 0.00032 (x99.99)
- `NYU/2/dwi.json` <- `NYU_2/dwi.pdf`: 4.20027e-06 -> 0.00032 (x76.19)
- `OHSU/1/bold.json` <- `OHSU_1/bold.pdf` (select `REST1`): 6.79939e-06 -> 0.0005 (x73.54)
- `UCD/1/bold.json` <- `UCD_1/bold.pdf`: 7.00045e-06 -> 0.00051 (x72.85)
- `UCLA/1/bold.json` <- `UCLA_1/bold.pdf`: 6.39844e-06 -> 0.0005123 (x80.07)
- `UCLA/Long/bold.json` <- `UCLA_Long/bold.pdf`: 6.39844e-06 -> 0.0005123 (x80.07)
- `UPSM/Long/bold.json` <- `UPSM_Long/bold.pdf`: 4.9984e-06 -> 0.00037 (x74.02)
- `USM/1/bold.json` <- `USM_1/bold.pdf`: 5.3991e-06 -> 0.000215 (x39.82)

### `DwellTime` dropped (10)

- `BNI/1/FLAIR.json` <- `BNI_1/3DFLAIR.txt`: was 0.0006840070042317234
- `BNI/1/T1w.json` <- `BNI_1/anat.txt`: was 0.003963097713097713
- `BNI/1/bold.json` <- `BNI_1/rest.txt`: was 0.014513788098693758
- `BNI/1/dwi.json` <- `BNI_1/dti.txt`: was 0.013236648250460404
- `EMC/1/T1w.json` <- `EMC_1/anat.pdf`: was 0.00018753000480076814
- `ETH/1/T1w.json` <- `ETH_1/anat.txt`: was 0.005310674455655868
- `ETH/1/bold.json` <- `ETH_1/rest.txt`: was 0.02531645569620253
- `KUL/3/T1w.json` <- `KUL_3/T1w.txt`: was 3.669994128009395e-05
- `SDSU/1/T1w.json` <- `SDSU_1/T1w.pdf`: was 1.599997542403775e-05
- `TCD/1/T1w.json` <- `TCD_1/T1w.txt`: was 2.0744822092405733e-05

### `DwellTime` factor-2 difference (10)

- `GU/1/T1w.json` <- `GU_1/T1w.pdf`: 2.05592e-05 -> 1.028e-05 (x0.50)
- `IU/1/T1w.json` <- `IU_1/T1w.pdf`: 1.4881e-05 -> 7.44e-06 (x0.50)
- `NYU/1/T1w.json` <- `NYU_1/T1w.pdf`: 1.95313e-05 -> 9.766e-06 (x0.50)
- `NYU/2/T1w.json` <- `NYU_2/T1w.pdf`: 1.95313e-05 -> 9.766e-06 (x0.50)
- `OHSU/1/T1w.json` <- `OHSU_1/T1w.pdf`: 2.17014e-05 -> 1.0851e-05 (x0.50)
- `UCD/1/T1w.json` <- `UCD_1/T1w.pdf`: 1.77557e-05 -> 8.878e-06 (x0.50)
- `UCLA/1/T1w.json` <- `UCLA_1/T1w.pdf`: 1.6276e-05 -> 8.138e-06 (x0.50)
- `UCLA/Long/T1w.json` <- `UCLA_Long/T1w.pdf`: 1.6276e-05 -> 8.138e-06 (x0.50)
- `UPSM/Long/T1w.json` <- `UPSM_Long/T1w.pdf`: 3.00481e-05 -> 1.5024e-05 (x0.50)
- `USM/1/T1w.json` <- `USM_1/T1w.pdf`: 1.6276e-05 -> 8.138e-06 (x0.50)

### `EffectiveEchoSpacing` dropped (8)

- `BNI/1/bold.json` <- `BNI_1/rest.txt`: was 0.00022328528524023913
- `BNI/1/dwi.json` <- `BNI_1/dti.txt`: was 0.00014138489711829105
- `EMC/1/T1w.json` <- `EMC_1/anat.pdf`: was 0.0
- `ETH/1/bold.json` <- `ETH_1/rest.txt`: was 0.0003106077448270799
- `IP/1/bold.json` <- `IP_1/bold.pdf`: was 7.172623530508754e-06
- `KUL/3/bold.json` <- `KUL_3/bold.txt`: was 5.859466554164909e-06
- `SDSU/1/bold.json` <- `SDSU_1/bold.pdf`: was 2e-06
- `TCD/1/dwi.json` <- `TCD_1/dwi.txt`: was 3.1129916347688794e-06

### `MagneticFieldStrength` dropped (6)

- `BNI/1/FLAIR.json` <- `BNI_1/3DFLAIR.txt`: was 3.0
- `BNI/1/dwi.json` <- `BNI_1/dti.txt`: was 3.0
- `SDSU/1/T1w.json` <- `SDSU_1/T1w.pdf`: was 3.0
- `SDSU/1/bold.json` <- `SDSU_1/bold.pdf`: was 3.0
- `SDSU/1/dwi.json` <- `SDSU_1/dwi.pdf`: was 3.0
- `TCD/1/dwi.json` <- `TCD_1/dwi.txt`: was 3.0

### `EchoTime` dropped (6)

- `BNI/1/FLAIR.json` <- `BNI_1/3DFLAIR.txt`: was 0.28300000000000003
- `BNI/1/dwi.json` <- `BNI_1/dti.txt`: was 0.101
- `IP/1/T1w.json` <- `IP_1/T1w.pdf`: was 0.0056
- `IP/1/dwi.json` <- `IP_1/dwi.pdf`: was 0.08600000000000001
- `SDSU/1/T1w.json` <- `SDSU_1/T1w.pdf`: was 0.0031720000000000003
- `TCD/1/dwi.json` <- `TCD_1/dwi.txt`: was 0.079

### `PhaseEncodingDirection` dropped (6)

- `BNI/1/FLAIR.json` <- `BNI_1/3DFLAIR.txt`: was "SI"
- `BNI/1/T1w.json` <- `BNI_1/anat.txt`: was "SI"
- `BNI/1/bold.json` <- `BNI_1/rest.txt`: was "RL"
- `BNI/1/dwi.json` <- `BNI_1/dti.txt`: was "RL"
- `ETH/1/T1w.json` <- `ETH_1/anat.txt`: was "AP"
- `ETH/1/bold.json` <- `ETH_1/rest.txt`: was "RL"

### no repetition time produced (6)

- `BNI/1/FLAIR.json` <- `BNI_1/3DFLAIR.txt`
- `BNI/1/dwi.json` <- `BNI_1/dti.txt`
- `IP/1/T1w.json` <- `IP_1/T1w.pdf`
- `IP/1/dwi.json` <- `IP_1/dwi.pdf`
- `SDSU/1/T1w.json` <- `SDSU_1/T1w.pdf`
- `TCD/1/dwi.json` <- `TCD_1/dwi.txt`

### `RepetitionTimePreparation` dropped (5)

- `BNI/1/FLAIR.json` <- `BNI_1/3DFLAIR.txt`: was 4.8
- `BNI/1/T1w.json` <- `BNI_1/anat.txt`: was 0.0067
- `EMC/1/T1w.json` <- `EMC_1/anat.pdf`: was 0.01027
- `IP/1/T1w.json` <- `IP_1/T1w.pdf`: was 0.025
- `SDSU/1/T1w.json` <- `SDSU_1/T1w.pdf`: was 0.008136

### `SliceThickness` dropped (5)

- `BNI/1/T1w.json` <- `BNI_1/anat.txt`: was 1.2
- `ETH/1/T1w.json` <- `ETH_1/anat.txt`: was 0.9
- `IP/1/T1w.json` <- `IP_1/T1w.pdf`: was 1.0
- `KUL/3/T1w.json` <- `KUL_3/T1w.txt`: was 1.2
- `TCD/1/T1w.json` <- `TCD_1/T1w.txt`: was 0.9

### `TotalReadoutTime` dropped (4)

- `BNI/1/bold.json` <- `BNI_1/rest.txt`: was 0.014066972970135065
- `BNI/1/dwi.json` <- `BNI_1/dti.txt`: was 0.02700451534959359
- `EMC/1/T1w.json` <- `EMC_1/anat.pdf`: was 0.0
- `ETH/1/bold.json` <- `ETH_1/rest.txt`: was 0.024538011841339313

### `ParallelReductionFactorInPlane` dropped (2)

- `EMC/1/bold.json` <- `EMC_1/rest.pdf`: was 1.0
- `IP/1/T1w.json` <- `IP_1/T1w.pdf`: was 2.0

### `EchoTime` differs (2)

- `NYU/1/bold.json` <- `NYU_1/bold.pdf`: 0.03 -> 0.015
- `NYU/2/bold.json` <- `NYU_2/bold.pdf`: 0.015 -> 0.03

### `FlipAngle` differs (2)

- `NYU/1/bold.json` <- `NYU_1/bold.pdf`: 82.0 -> 90.0
- `NYU/2/bold.json` <- `NYU_2/bold.pdf`: 90.0 -> 82.0

### `FlipAngle` dropped (2)

- `NYU/1/dwi.json` <- `NYU_1/dwi.pdf`: was 60.0
- `NYU/2/dwi.json` <- `NYU_2/dwi.pdf`: was 60.0

### `SliceTiming` dropped (1)

- `BNI/1/T1w.json` <- `BNI_1/anat.txt`: was "Sequential"

### `InversionTime` dropped (1)

- `EMC/1/bold.json` <- `EMC_1/rest.pdf`: was 0.0

## Non-BIDS keys dropped from the old templates (not re-added)

- `AcquisitionMatrixFE` (8): `BNI/1/FLAIR.json`, `BNI/1/T1w.json`, `BNI/1/bold.json`, `BNI/1/dwi.json`, `EMC/1/T1w.json`, `EMC/1/bold.json`, `ETH/1/T1w.json`, `ETH/1/bold.json`
- `AcquisitionMatrixSE` (41): `BNI/1/FLAIR.json`, `BNI/1/T1w.json`, `BNI/1/bold.json`, `BNI/1/dwi.json`, `EMC/1/T1w.json`, `EMC/1/bold.json`, `ETH/1/T1w.json`, `ETH/1/bold.json`, `GU/1/T1w.json`, `GU/1/bold.json`, `IP/1/T1w.json`, `IP/1/bold.json`, `IP/1/dwi.json`, `IU/1/T1w.json`, `IU/1/bold.json`, `KUL/3/T1w.json`, `KUL/3/bold.json`, `NYU/1/T1w.json`, `NYU/1/bold.json`, `NYU/1/dwi.json`, `NYU/2/T1w.json`, `NYU/2/bold.json`, `NYU/2/dwi.json`, `OHSU/1/T1w.json`, `OHSU/1/bold.json`, `SDSU/1/T1w.json`, `SDSU/1/bold.json`, `SDSU/1/dwi.json`, `TCD/1/T1w.json`, `TCD/1/bold.json`, `TCD/1/dwi.json`, `UCD/1/T1w.json`, `UCD/1/bold.json`, `UCLA/1/T1w.json`, `UCLA/1/bold.json`, `UCLA/Long/T1w.json`, `UCLA/Long/bold.json`, `UPSM/Long/T1w.json`, `UPSM/Long/bold.json`, `USM/1/T1w.json`, `USM/1/bold.json`
- `FatSaturation` (38): `BNI/1/FLAIR.json`, `BNI/1/T1w.json`, `BNI/1/bold.json`, `BNI/1/dwi.json`, `EMC/1/T1w.json`, `ETH/1/T1w.json`, `ETH/1/bold.json`, `GU/1/T1w.json`, `GU/1/bold.json`, `IP/1/T1w.json`, `IP/1/bold.json`, `IU/1/T1w.json`, `IU/1/bold.json`, `KUL/3/T1w.json`, `KUL/3/bold.json`, `NYU/1/T1w.json`, `NYU/1/bold.json`, `NYU/1/dwi.json`, `NYU/2/T1w.json`, `NYU/2/bold.json`, `NYU/2/dwi.json`, `OHSU/1/T1w.json`, `OHSU/1/bold.json`, `SDSU/1/T1w.json`, `SDSU/1/bold.json`, `SDSU/1/dwi.json`, `TCD/1/T1w.json`, `TCD/1/bold.json`, `UCD/1/T1w.json`, `UCD/1/bold.json`, `UCLA/1/T1w.json`, `UCLA/1/bold.json`, `UCLA/Long/T1w.json`, `UCLA/Long/bold.json`, `UPSM/Long/T1w.json`, `UPSM/Long/bold.json`, `USM/1/T1w.json`, `USM/1/bold.json`
- `FreqEncodingDirection` (6): `BNI/1/FLAIR.json`, `BNI/1/T1w.json`, `BNI/1/bold.json`, `BNI/1/dwi.json`, `ETH/1/T1w.json`, `ETH/1/bold.json`
- `ReconMatrixFE` (8): `BNI/1/FLAIR.json`, `BNI/1/T1w.json`, `BNI/1/bold.json`, `BNI/1/dwi.json`, `EMC/1/T1w.json`, `EMC/1/bold.json`, `ETH/1/T1w.json`, `ETH/1/bold.json`
- `ReconMatrixSE` (41): `BNI/1/FLAIR.json`, `BNI/1/T1w.json`, `BNI/1/bold.json`, `BNI/1/dwi.json`, `EMC/1/T1w.json`, `EMC/1/bold.json`, `ETH/1/T1w.json`, `ETH/1/bold.json`, `GU/1/T1w.json`, `GU/1/bold.json`, `IP/1/T1w.json`, `IP/1/bold.json`, `IP/1/dwi.json`, `IU/1/T1w.json`, `IU/1/bold.json`, `KUL/3/T1w.json`, `KUL/3/bold.json`, `NYU/1/T1w.json`, `NYU/1/bold.json`, `NYU/1/dwi.json`, `NYU/2/T1w.json`, `NYU/2/bold.json`, `NYU/2/dwi.json`, `OHSU/1/T1w.json`, `OHSU/1/bold.json`, `SDSU/1/T1w.json`, `SDSU/1/bold.json`, `SDSU/1/dwi.json`, `TCD/1/T1w.json`, `TCD/1/bold.json`, `TCD/1/dwi.json`, `UCD/1/T1w.json`, `UCD/1/bold.json`, `UCLA/1/T1w.json`, `UCLA/1/bold.json`, `UCLA/Long/T1w.json`, `UCLA/Long/bold.json`, `UPSM/Long/T1w.json`, `UPSM/Long/bold.json`, `USM/1/T1w.json`, `USM/1/bold.json`
- `VariableFlipAngleFlag` (2): `EMC/1/T1w.json`, `EMC/1/bold.json`
- `WaterFatShift` (4): `BNI/1/FLAIR.json`, `BNI/1/T1w.json`, `BNI/1/dwi.json`, `ETH/1/T1w.json`
