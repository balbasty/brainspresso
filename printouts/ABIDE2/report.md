# ABIDE2 sidecar regeneration report

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

protocol2bids 0.0.0 (commit 5b826707154c64f1cf256572f00a158a54e3da27)

Generated by `scripts/make_sidecars.py`; diffs are against the sidecar at git HEAD when the script was run.

## BNI/1/FLAIR.json

- added `Modality`: "MR"
- added `ProtocolName`: "3DFLAIR"
- added `ScanningSequence`: ["IR"]
- added `PhaseEncodingAxis`: "i"
- removed `MagneticFieldStrength`: 3.0
- removed `ReconMatrixFE`: 256
- removed `ReconMatrixSE`: 326
- removed `AcquisitionMatrixSE`: 326
- removed `AcquisitionMatrixFE`: 240
- removed `FreqEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "RL"
- removed `PhaseEncodingDirection`: "SI"
- removed `EchoTime`: 0.28300000000000003
- removed `PixelBandwidth`: 1370.6
- removed `FatSaturation`: true
- removed `RepetitionTimePreparation`: 4.8
- removed `WaterFatShift`: 0.317
- removed `DwellTime`: 0.0006840070042317234
- changed `PulseSequenceType`: "FLAIR" -> "Inversion Recovery Turbo Spin Echo"

## BNI/1/T1w.json

- added `ImagingFrequency`: 127.728
- added `Modality`: "MR"
- added `ProtocolName`: "anat"
- added `RepetitionTime`: 0.0067
- added `ScanningSequence`: ["GR", "IR"]
- added `RepetitionTimeExcitation`: 0.0067
- added `PhaseEncodingAxis`: "j"
- removed `ManufacturerModelName`: "Ingenia"
- removed `SliceEncodingDirection`: "RL"
- removed `SliceThickness`: 1.2
- removed `SliceTiming`: "Sequential"
- removed `FatSaturation`: false
- removed `AcquisitionMatrixSE`: 170
- removed `ReconMatrixSE`: 170
- removed `PixelBandwidth`: 240.5
- removed `DwellTime`: 0.003963097713097713
- removed `ReconMatrixFE`: 256
- removed `AcquisitionMatrixFE`: 244
- removed `FreqEncodingDirection`: "AP"
- removed `PhaseEncodingDirection`: "SI"
- removed `WaterFatShift`: 1.79999995
- removed `RepetitionTimePreparation`: 0.0067
- changed `PulseSequenceType`: "Fast Spoiled Gradient Echo" -> "Inversion Recovery Turbo Field Echo (MPRAGE-like)"
- changed `ParallelReductionFactorOutOfPlane`: 1.79999995 -> 1.8

## BNI/1/bold.json

- added `ImagingFrequency`: 127.728
- added `Modality`: "MR"
- added `ProtocolName`: "rest"
- added `RepetitionTime`: 3.0
- added `ScanningSequence`: ["EP", "GR"]
- added `PhaseEncodingAxis`: "j"
- added `EchoTrainLength`: 31
- added `EstimatedTotalReadoutTime`: 0.014066972970135067
- added `EstimatedEffectiveEchoSpacing`: 0.00022328528524023916
- removed `ManufacturerModelName`: "Ingenia"
- removed `SliceEncodingDirection`: "SI"
- removed `FatSaturation`: false
- removed `AcquisitionMatrixSE`: 50
- removed `ReconMatrixSE`: 50
- removed `RepetitionTimeExcitation`: 3.0
- removed `BandwidthPerPixelPhaseEncode`: 3280.0
- removed `EffectiveEchoSpacing`: 0.00022328528524023913
- removed `ReconMatrixFE`: 64
- removed `AcquisitionMatrixFE`: 64
- removed `FreqEncodingDirection`: "AP"
- removed `PhaseEncodingDirection`: "RL"
- removed `PixelBandwidth`: 68.9
- removed `DwellTime`: 0.014513788098693758
- removed `TotalReadoutTime`: 0.014066972970135065
- changed `SpacingBetweenSlices`: 0 -> 4.0

## BNI/1/dwi.json

- added `Modality`: "MR"
- added `ProtocolName`: "dti"
- added `ScanningSequence`: ["EP", "SE"]
- added `SliceThickness`: 3.0
- added `PhaseEncodingAxis`: "j"
- removed `MagneticFieldStrength`: 3.0
- removed `ReconMatrixFE`: 192
- removed `ReconMatrixSE`: 48
- removed `AcquisitionMatrixSE`: 48
- removed `AcquisitionMatrixFE`: 92
- removed `FreqEncodingDirection`: "AP"
- removed `SliceEncodingDirection`: "SI"
- removed `PhaseEncodingDirection`: "RL"
- removed `EchoTime`: 0.101
- removed `PixelBandwidth`: 36.2
- removed `WaterFatShift`: 11.988
- removed `FatSaturation`: true
- removed `RepetitionTimeExcitation`: 0.101
- removed `BandwidthPerPixelPhaseEncode`: 2621.1
- removed `EffectiveEchoSpacing`: 0.00014138489711829105
- removed `TotalReadoutTime`: 0.02700451534959359
- removed `DiffusionDelta`: [50.7, 29.8]
- removed `DwellTime`: 0.013236648250460404
- changed `SpacingBetweenSlices`: 0 -> 3.0

## EMC/1/T1w.json

- added `PulseSequenceName`: "efgre3d"
- added `Modality`: "MR"
- added `RepetitionTime`: 0.01026
- added `SpacingBetweenSlices`: 0.9
- added `EchoTrainLength`: 1
- added `SAR`: 0.267604
- added `PhaseEncodingAxis`: "i"
- removed `ManufacturerModelName`: "MR750"
- removed `PulseSequenceType`: "IR-FSPGR"
- removed `ParallelAcquisitionTechnique`: "ASSET"
- removed `SliceEncodingDirection`: "A"
- removed `FatSaturation`: false
- removed `AcquisitionMatrixSE`: 186
- removed `ReconMatrixSE`: 186
- removed `RepetitionTimePreparation`: 0.01027
- removed `DwellTime`: 0.00018753000480076814
- removed `DeviceSerialNumber`: "000000000RTD0125"
- removed `StationName`: "MR750K10"
- removed `InstitutionName`: "EMC"
- removed `ParallelReductionFactorOutOfPlane`: 1.0
- removed `RepetitionTimeExcitation`: 0.01026
- removed `ProcedureStepDescription`: "GEN R"
- removed `AcquisitionTime`: "171431.000000"
- removed `AcquisitionNumber`: 1
- removed `VariableFlipAngleFlag`: true
- removed `AcquisitionMatrixFE`: 256
- removed `ReconMatrixFE`: 512
- removed `EffectiveEchoSpacing`: 0.0
- removed `TotalReadoutTime`: 0.0
- changed `Manufacturer`: "GE MEDICAL SYSTEMS" -> "GE"
- changed `ScanningSequence`: "GR" -> ["GR"]
- changed `SequenceVariant`: "SS\\SP\\SK" -> ["SS", "SP", "SK"]
- changed `ScanOptions`: "FAST_GEMS\\ACC_GEMS" -> ["FAST_GEMS", "ACC_GEMS"]
- changed `ImageType`: "ORIGINAL\\PRIMARY\\OTHER" -> ["ORIGINAL", "PRIMARY", "OTHER"]
- changed `ImagingFrequency`: "127.705356" -> 127.705356
- changed `PatientPosition`: "83.962\\-124.462\\125.937" -> "HFS"
- changed `ImageOrientationPatientDICOM`: "0.0890827\\0.996023\\-0.00146486\\-0.0114139\\-0.0004577" -> [0.0890827, 0.996023, -0.00146486, -0.0114139, -0.00045776]

## EMC/1/bold.json

- added `PulseSequenceName`: "epi"
- added `Modality`: "MR"
- added `RepetitionTime`: 2.0
- added `SpacingBetweenSlices`: 4.0
- added `EchoTrainLength`: 1
- added `SAR`: 0.317719
- added `PhaseEncodingAxis`: "j"
- removed `ManufacturerModelName`: "MR750"
- removed `PulseSequenceType`: "Gradient Echo EPI"
- removed `SliceEncodingDirection`: "A"
- removed `AcquisitionMatrixSE`: 37
- removed `ReconMatrixSE`: 37
- removed `RepetitionTimeExcitation`: 2.0
- removed `DeviceSerialNumber`: "000000000RTD0125"
- removed `StationName`: "MR750K10"
- removed `InstitutionName`: "EMC"
- removed `ParallelReductionFactorInPlane`: 1.0
- removed `ParallelReductionFactorOutOfPlane`: 1.0
- removed `InversionTime`: 0.0
- removed `ProcedureStepDescription`: "GEN R"
- removed `AcquisitionTime`: "173710.000000"
- removed `AcquisitionNumber`: 1
- removed `VariableFlipAngleFlag`: true
- removed `AcquisitionMatrixFE`: 64
- removed `ReconMatrixFE`: 64
- changed `Manufacturer`: "GE MEDICAL SYSTEMS" -> "GE"
- changed `ScanningSequence`: "EP\\GR" -> ["EP", "GR"]
- changed `SequenceVariant`: "SS" -> ["SS"]
- changed `ScanOptions`: "MP_GEMS\\EPI_GEMS" -> ["MP_GEMS", "EPI_GEMS"]
- changed `ImageType`: "ORIGINAL\\PRIMARY\\OTHER" -> ["ORIGINAL", "PRIMARY", "OTHER"]
- changed `ImagingFrequency`: "127.705394" -> 127.705394
- changed `PatientPosition`: "-101.249\\-145.186\\78.6314" -> "HFS"
- changed `ImageOrientationPatientDICOM`: "1\\-0\\0\\-0\\1\\0" -> [1.0, -0.0, 0.0, -0.0, 1.0, 0.0]

## ETH/1/T1w.json

- added `ImagingFrequency`: 127.728
- added `Modality`: "MR"
- added `ProtocolName`: "anat"
- added `RepetitionTime`: 0.008400000000000001
- added `ScanningSequence`: ["GR", "IR"]
- added `RepetitionTimeExcitation`: 0.008400000000000001
- added `PhaseEncodingAxis`: "i"
- removed `ManufacturerModelName`: "Achieva"
- removed `SliceEncodingDirection`: "SI"
- removed `SliceThickness`: 0.9
- removed `FatSaturation`: false
- removed `AcquisitionMatrixSE`: 180
- removed `ReconMatrixSE`: 180
- removed `PixelBandwidth`: 188.3
- removed `DwellTime`: 0.005310674455655868
- removed `ReconMatrixFE`: 256
- removed `AcquisitionMatrixFE`: 256
- removed `FreqEncodingDirection`: "RL"
- removed `PhaseEncodingDirection`: "AP"
- removed `WaterFatShift`: 2.29999995
- changed `RepetitionTimePreparation`: 0.00839999962 -> 3.0
- changed `PulseSequenceType`: "Fast Spoiled Gradient Echo" -> "Inversion Recovery Turbo Field Echo (MPRAGE-like)"
- changed `ParallelReductionFactorInPlane`: 2.29999995 -> 2.3

## ETH/1/bold.json

- added `ImagingFrequency`: 127.728
- added `Modality`: "MR"
- added `ProtocolName`: "rest"
- added `RepetitionTime`: 2.0
- added `ScanningSequence`: ["EP", "GR"]
- added `PhaseEncodingAxis`: "j"
- added `EchoTrainLength`: 31
- added `EstimatedTotalReadoutTime`: 0.024538011841339316
- added `EstimatedEffectiveEchoSpacing`: 0.00031060774482707997
- removed `ManufacturerModelName`: "Achieva"
- removed `SliceEncodingDirection`: "SI"
- removed `FatSaturation`: true
- removed `AcquisitionMatrixSE`: 40
- removed `ReconMatrixSE`: 40
- removed `RepetitionTimeExcitation`: 2.0
- removed `ReconMatrixFE`: 80
- removed `AcquisitionMatrixFE`: 80
- removed `FreqEncodingDirection`: "AP"
- removed `PhaseEncodingDirection`: "RL"
- removed `PixelBandwidth`: 39.5
- removed `DwellTime`: 0.02531645569620253
- removed `BandwidthPerPixelPhaseEncode`: 1589.9
- removed `EffectiveEchoSpacing`: 0.0003106077448270799
- removed `TotalReadoutTime`: 0.024538011841339313
- changed `SpacingBetweenSlices`: 0.299999952 -> 3.3

## GU/1/T1w.json

- added `ManufacturersModelName`: "TrioTim"
- added `SoftwareVersions`: "syngo MR B17"
- added `ReceiveCoilActiveElements`: "PH"
- added `MatrixCoilMode`: "Dual"
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
- added `ParallelReductionFactorOutOfPlane`: 1
- added `RepetitionTime`: 2.5300000000000002
- added `RepetitionTimeExcitation`: 0.008199999999999999
- added `Modality`: "MR"
- added `ProtocolName`: "MGH_MPRAGE 1mm iPAT 128PE-Ref"
- added `SeriesDescription`: "MGH_MPRAGE 1mm iPAT 128PE-Ref"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 256
- added `AcquisitionMatrixPE`: 256
- added `ReconMatrixPE`: 256
- added `PercentPhaseFOV`: 100.0
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 256
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TriTim"
- removed `FatSaturation`: false
- removed `AcquisitionMatrixSE`: 176
- removed `ReconMatrixSE`: 176
- changed `SliceEncodingDirection`: "A" -> "i"
- changed `DwellTime`: 2.055921052631579e-05 -> 1.028e-05
- changed `PixelBandwidth`: "190" -> 190.0

## GU/1/bold.json

- added `ManufacturersModelName`: "TrioTim"
- added `SoftwareVersions`: "syngo MR B17"
- added `ReceiveCoilActiveElements`: "HEA;HEP"
- added `MatrixCoilMode`: "Auto (Triple)"
- added `CoilCombinationMethod`: "rSOS"
- added `ScanningSequence`: ["EP"]
- added `SequenceVariant`: ["SK"]
- added `ScanOptions`: []
- added `SequenceName`: "ep2d_bold"
- added `PulseSequenceDetails`: "%SiemensSeq%\\ep2d_bold"
- added `MRAcquisitionType`: "2D"
- added `MTState`: false
- added `B0ShimmingTechnique`: "Standard"
- added `NumberShots`: 1
- added `TotalReadoutTime`: 0.015434999999999999
- added `SliceTiming`: [0.0, 1.023256, 0.046512, 1.069767, 0.093023, 1.116279, 0.139535, 1.162791, 0.186047, 1.209302, 0.232558, 1.255814, 0.27907, 1.302326, 0.325581, 1.348837, 0.372093, 1.395349, 0.418605, 1.44186, 0.465116, 1.488372, 0.511628, 1.534884, 0.55814, 1.581395, 0.604651, 1.627907, 0.651163, 1.674419, 0.697674, 1.72093, 0.744186, 1.767442, 0.790698, 1.813953, 0.837209, 1.860465, 0.883721, 1.906977, 0.930233, 1.953488, 0.976744]
- added `DwellTime`: 3.199e-06
- added `RepetitionTime`: 2.0
- added `Modality`: "MR"
- added `ProtocolName`: "ep2d 3mm3, rest"
- added `SeriesDescription`: "ep2d 3mm3, rest"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 64
- added `AcquisitionMatrixPE`: 64
- added `ReconMatrixPE`: 64
- added `PercentPhaseFOV`: 100.0
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 64
- added `EchoTrainLength`: 32
- added `PixelBandwidth`: 2442.0
- added `DerivedVendorReportedEchoSpacing`: 0.00049
- added `SpacingBetweenSlices`: 3.0
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TriTim"
- removed `SliceEncodingDirection`: "A"
- removed `FatSaturation`: false
- removed `AcquisitionMatrixSE`: 43
- removed `ReconMatrixSE`: 43
- removed `RepetitionTimeExcitation`: 2.0
- changed `EffectiveEchoSpacing`: 6.398443898443899e-06 -> 0.000245
- changed `BandwidthPerPixelPhaseEncode`: "2442" -> 63.775510204081634

## IP/1/T1w.json

- added `ManufacturersModelName`: "Intera Achieva"
- added `ImagingFrequency`: 63.864
- added `Modality`: "MR"
- added `ProtocolName`: "T1w"
- added `ScanningSequence`: ["GR"]
- added `AcquisitionMatrixPE`: 240
- added `PhaseEncodingAxis`: "j"
- removed `ManufacturerModelName`: "Achieva"
- removed `EchoTime`: 0.0056
- removed `ParallelReductionFactorInPlane`: 2.0
- removed `ParallelAcquisitionTechnique`: "SENSE"
- removed `SliceEncodingDirection`: "A"
- removed `SliceThickness`: 1.0
- removed `FatSaturation`: false
- removed `AcquisitionMatrixSE`: 170
- removed `ReconMatrixSE`: 170
- removed `RepetitionTimePreparation`: 0.025
- changed `PulseSequenceType`: "MPRAGE" -> "Fast Field Echo"

## IP/1/bold.json

- added `ManufacturersModelName`: "Intera Achieva"
- added `ImagingFrequency`: 63.864
- added `Modality`: "MR"
- added `ProtocolName`: "bold"
- added `RepetitionTime`: 2.7
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP", "GR"]
- added `AcquisitionMatrixPE`: 63
- added `ReconMatrixPE`: 64
- added `SpacingBetweenSlices`: 4.0
- added `PhaseEncodingAxis`: "j"
- added `WaterFatShift`: 9.317
- added `EchoTrainLength`: 63
- added `EstimatedTotalReadoutTime`: 0.042237833866635724
- added `EstimatedEffectiveEchoSpacing`: 0.0006704418074069162
- removed `ManufacturerModelName`: "Achieva"
- removed `SliceEncodingDirection`: "A"
- removed `FatSaturation`: true
- removed `AcquisitionMatrixSE`: 32
- removed `ReconMatrixSE`: 32
- removed `RepetitionTimeExcitation`: 2.7
- removed `BandwidthPerPixelPhaseEncode`: "2213"
- removed `EffectiveEchoSpacing`: 7.172623530508754e-06

## IP/1/dwi.json

- added `ManufacturersModelName`: "Intera Achieva"
- added `ImagingFrequency`: 63.864
- added `Modality`: "MR"
- added `ProtocolName`: "dwi"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP", "SE"]
- added `AcquisitionMatrixPE`: 74
- added `ReconMatrixPE`: 96
- added `PartialFourierEnabled`: "YES"
- added `PartialFourier`: 0.683
- added `SpacingBetweenSlices`: 2.5
- added `PhaseEncodingAxis`: "j"
- removed `ManufacturerModelName`: "Achieva"
- removed `EchoTime`: 0.08600000000000001
- removed `SliceEncodingDirection`: "A"
- removed `AcquisitionMatrixSE`: 45
- removed `ReconMatrixSE`: 45
- removed `RepetitionTimeExcitation`: 5.407

## IU/1/T1w.json

- added `ManufacturersModelName`: "TrioTim"
- added `SoftwareVersions`: "syngo MR B17"
- added `ReceiveCoilActiveElements`: "HEA;HEP"
- added `MatrixCoilMode`: "Auto (Triple)"
- added `CoilCombinationMethod`: "Adaptive Combine"
- added `ScanningSequence`: ["GR", "IR"]
- added `SequenceVariant`: ["SK", "SP", "MP"]
- added `ScanOptions`: ["IR", "PFP"]
- added `SequenceName`: "tfl"
- added `PulseSequenceDetails`: "%SiemensSeq%\\tfl"
- added `SpoilingState`: true
- added `SpoilingType`: "COMBINED"
- added `SpoilingRFPhaseIncrement`: 50.0
- added `B0ShimmingTechnique`: "Standard"
- added `ParallelReductionFactorOutOfPlane`: 1
- added `PartialFourier`: 0.875
- added `PartialFourierDirection`: "PHASE"
- added `RepetitionTime`: 2.4
- added `RepetitionTimeExcitation`: 0.007
- added `Modality`: "MR"
- added `ProtocolName`: "tfl3d_nsIR_sag"
- added `SeriesDescription`: "tfl3d_nsIR_sag"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 320
- added `AcquisitionMatrixPE`: 320
- added `ReconMatrixPE`: 320
- added `PercentPhaseFOV`: 100.0
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 280
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TriTim"
- removed `FatSaturation`: false
- removed `AcquisitionMatrixSE`: 256
- removed `ReconMatrixSE`: 256
- changed `PulseSequenceType`: "TFL" -> "MPRAGE"
- changed `SliceEncodingDirection`: "A" -> "i"
- changed `DwellTime`: 1.4880952380952381e-05 -> 7.440000000000001e-06
- changed `PixelBandwidth`: "210" -> 210.0

## IU/1/bold.json

- added `ManufacturersModelName`: "TrioTim"
- added `SoftwareVersions`: "syngo MR B17"
- added `ReceiveCoilActiveElements`: "HEA;HEP"
- added `MatrixCoilMode`: "Auto (CP)"
- added `CoilCombinationMethod`: "rSOS"
- added `ScanningSequence`: ["EP"]
- added `SequenceVariant`: ["SK", "SS"]
- added `ScanOptions`: ["FS"]
- added `SequenceName`: "cmrr_mbep2d_bold"
- added `PulseSequenceDetails`: "%CustomerSeq%\\cmrr_mbep2d_bold"
- added `MRAcquisitionType`: "2D"
- added `MTState`: false
- added `B0ShimmingTechnique`: "Standard"
- added `NumberShots`: 1
- added `PhaseEncodingDirection`: "j-"
- added `TotalReadoutTime`: 0.030869999999999998
- added `SliceTiming`: [0.0, 0.174214, 0.348429, 0.522643, 0.696857, 0.058071, 0.232286, 0.4065, 0.580714, 0.754929, 0.116143, 0.290357, 0.464571, 0.638786, 0.0, 0.174214, 0.348429, 0.522643, 0.696857, 0.058071, 0.232286, 0.4065, 0.580714, 0.754929, 0.116143, 0.290357, 0.464571, 0.638786, 0.0, 0.174214, 0.348429, 0.522643, 0.696857, 0.058071, 0.232286, 0.4065, 0.580714, 0.754929, 0.116143, 0.290357, 0.464571, 0.638786]
- added `SliceEncodingDirection`: "k"
- added `DwellTime`: 3e-06
- added `MultibandAccelerationFactor`: 3
- added `RepetitionTime`: 0.8130000000000001
- added `Modality`: "MR"
- added `ProtocolName`: "cmrr_mb3ep2d_bold_restingstate"
- added `SeriesDescription`: "cmrr_mb3ep2d_bold_restingstate"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 64
- added `AcquisitionMatrixPE`: 64
- added `ReconMatrixPE`: 64
- added `PercentPhaseFOV`: 100.0
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 64
- added `EchoTrainLength`: 64
- added `PixelBandwidth`: 2604.0
- added `DerivedVendorReportedEchoSpacing`: 0.00049
- added `SpacingBetweenSlices`: 3.4
- removed `ManufacturerModelName`: "TriTim"
- removed `FatSaturation`: true
- removed `AcquisitionMatrixSE`: 42
- removed `ReconMatrixSE`: 42
- removed `RepetitionTimeExcitation`: 0.8130000000000001
- changed `PulseSequenceType`: "Gradient Echo EPI" -> "Multiband Gradient Echo EPI"
- changed `EffectiveEchoSpacing`: 6.000384024577573e-06 -> 0.00049
- changed `BandwidthPerPixelPhaseEncode`: "2604" -> 31.887755102040817

## KUL/3/T1w.json

- added `ImagingFrequency`: 127.728
- added `Modality`: "MR"
- added `ProtocolName`: "T1w"
- added `RepetitionTime`: 0.0094
- added `ScanningSequence`: ["GR", "IR"]
- added `RepetitionTimeExcitation`: 0.0094
- added `ParallelReductionFactorInPlane`: 1.5
- added `ParallelReductionFactorOutOfPlane`: 2.5
- added `AcquisitionMatrixPE`: 207
- added `ReconMatrixPE`: 256
- added `PhaseEncodingAxis`: "i"
- removed `ManufacturerModelName`: "Achieva Ds"
- removed `SliceEncodingDirection`: "A"
- removed `SliceThickness`: 1.2
- removed `FatSaturation`: false
- removed `AcquisitionMatrixSE`: 182
- removed `ReconMatrixSE`: 182
- removed `PixelBandwidth`: "131"
- removed `DwellTime`: 3.669994128009395e-05
- changed `RepetitionTimePreparation`: 0.0094 -> 2.0
- changed `PulseSequenceType`: "TFE" -> "Inversion Recovery Turbo Field Echo (MPRAGE-like)"

## KUL/3/bold.json

- added `ImagingFrequency`: 127.728
- added `Modality`: "MR"
- added `ProtocolName`: "bold"
- added `RepetitionTime`: 2.5
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP", "GR"]
- added `ParallelReductionFactorInPlane`: 2.0
- added `AcquisitionMatrixPE`: 78
- added `ReconMatrixPE`: 128
- added `SpacingBetweenSlices`: 3.1
- added `PhaseEncodingAxis`: "j"
- added `WaterFatShift`: 10.56
- added `EchoTrainLength`: 39
- added `EstimatedTotalReadoutTime`: 0.023708468731348235
- added `EstimatedEffectiveEchoSpacing`: 0.0001866808561523483
- added `SliceTiming`: [0.0, 0.055556, 0.111111, 0.166667, 0.222222, 0.277778, 0.333333, 0.388889, 0.444444, 0.5, 0.555556, 0.611111, 0.666667, 0.722222, 0.777778, 0.833333, 0.888889, 0.944444, 1.0, 1.055556, 1.111111, 1.166667, 1.222222, 1.277778, 1.333333, 1.388889, 1.444444, 1.5, 1.555556, 1.611111, 1.666667, 1.722222, 1.777778, 1.833333, 1.888889, 1.944444, 2.0, 2.055556, 2.111111, 2.166667, 2.222222, 2.277778, 2.333333, 2.388889, 2.444444]
- removed `ManufacturerModelName`: "Achieva Ds"
- removed `SliceEncodingDirection`: "A"
- removed `FatSaturation`: true
- removed `AcquisitionMatrixSE`: 45
- removed `ReconMatrixSE`: 45
- removed `RepetitionTimeExcitation`: 2.5
- removed `BandwidthPerPixelPhaseEncode`: "2188"
- removed `EffectiveEchoSpacing`: 5.859466554164909e-06
- changed `SliceThickness`: 2.7 -> 2.70000005

## NYU/1/T1w.json

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
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "Allegra"
- removed `FatSaturation`: false
- removed `AcquisitionMatrixSE`: 128
- removed `ReconMatrixSE`: 128
- changed `PulseSequenceType`: "TFL" -> "MPRAGE"
- changed `SliceEncodingDirection`: "A" -> "i"
- changed `DwellTime`: 1.953125e-05 -> 9.766000000000001e-06
- changed `RepetitionTimePreparation`: 0.0032500000000000003 -> 2.5300000000000002
- changed `PixelBandwidth`: "200" -> 200.0

## NYU/1/bold.json

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
- added `PhaseEncodingDirection`: "i"
- added `TotalReadoutTime`: 0.02016
- added `SliceTiming`: [0.0, 1.030303, 0.060606, 1.090909, 0.121212, 1.151515, 0.181818, 1.212121, 0.242424, 1.272727, 0.30303, 1.333333, 0.363636, 1.393939, 0.424242, 1.454545, 0.484848, 1.515152, 0.545455, 1.575758, 0.606061, 1.636364, 0.666667, 1.69697, 0.727273, 1.757576, 0.787879, 1.818182, 0.848485, 1.878788, 0.909091, 1.939394, 0.969697]
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
- added `DerivedVendorReportedEchoSpacing`: 0.00032
- added `SpacingBetweenSlices`: 4.0
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "Allegra"
- removed `FatSaturation`: true
- removed `AcquisitionMatrixSE`: 34
- removed `ReconMatrixSE`: 34
- removed `RepetitionTimeExcitation`: 2.0
- changed `EffectiveEchoSpacing`: 3.200204813108039e-06 -> 0.00032
- changed `EchoTime`: 0.03 -> 0.015
- changed `SliceEncodingDirection`: "A" -> "k"
- changed `FlipAngle`: 82.0 -> 90.0
- changed `BandwidthPerPixelPhaseEncode`: "3906" -> 48.82812499999999
- changed `SliceThickness`: 3.0 -> 4.0

## NYU/1/dwi.json

- added `ManufacturersModelName`: "Allegra"
- added `SoftwareVersions`: "syngo MR 2004A"
- added `ReceiveCoilActiveElements`: "TR"
- added `ScanningSequence`: ["EP"]
- added `SequenceVariant`: ["SK", "SP"]
- added `ScanOptions`: ["FS"]
- added `SequenceName`: "cbi_ep2d_diff_1_4"
- added `PulseSequenceDetails`: "%CustomerSeq%\\cbi_ep2d_diff_1_4"
- added `MRAcquisitionType`: "2D"
- added `MTState`: false
- added `B0ShimmingTechnique`: "Standard"
- added `NumberShots`: 1
- added `PhaseEncodingDirection`: "i"
- added `TotalReadoutTime`: 0.02016
- added `SliceTiming`: [2.6, 0.0, 2.704, 0.104, 2.808, 0.208, 2.912, 0.312, 3.016, 0.416, 3.12, 0.52, 3.224, 0.624, 3.328, 0.728, 3.432, 0.832, 3.536, 0.936, 3.64, 1.04, 3.744, 1.144, 3.848, 1.248, 3.952, 1.352, 4.056, 1.456, 4.16, 1.56, 4.264, 1.664, 4.368, 1.768, 4.472, 1.872, 4.576, 1.976, 4.68, 2.08, 4.784, 2.184, 4.888, 2.288, 4.992, 2.392, 5.096, 2.496]
- added `DwellTime`: 2.1000000000000002e-06
- added `RepetitionTime`: 5.2
- added `Modality`: "MR"
- added `ProtocolName`: "dti_64dir_3x3x3"
- added `SeriesDescription`: "dti_64dir_3x3x3"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 64
- added `AcquisitionMatrixPE`: 64
- added `ReconMatrixPE`: 64
- added `PercentPhaseFOV`: 100.0
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 64
- added `EchoTrainLength`: 64
- added `PixelBandwidth`: 3720.0
- added `DerivedVendorReportedEchoSpacing`: 0.00032
- added `SpacingBetweenSlices`: 3.0
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "Allegra"
- removed `FlipAngle`: 60.0
- removed `FatSaturation`: false
- removed `AcquisitionMatrixSE`: 50
- removed `ReconMatrixSE`: 50
- removed `RepetitionTimeExcitation`: 5.2
- changed `EffectiveEchoSpacing`: 4.200268817204301e-06 -> 0.00032
- changed `SliceEncodingDirection`: "A" -> "k"
- changed `BandwidthPerPixelPhaseEncode`: "3720" -> 48.82812499999999

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
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "Allegra"
- removed `FatSaturation`: false
- removed `AcquisitionMatrixSE`: 128
- removed `ReconMatrixSE`: 128
- changed `PulseSequenceType`: "TFL" -> "MPRAGE"
- changed `SliceEncodingDirection`: "A" -> "i"
- changed `DwellTime`: 1.953125e-05 -> 9.766000000000001e-06
- changed `RepetitionTimePreparation`: 0.0032500000000000003 -> 2.5300000000000002
- changed `PixelBandwidth`: "200" -> 200.0

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
- added `PhaseEncodingDirection`: "i"
- added `TotalReadoutTime`: 0.02016
- added `SliceTiming`: [1.0, 0.0, 1.058824, 0.058824, 1.117647, 0.117647, 1.176471, 0.176471, 1.235294, 0.235294, 1.294118, 0.294118, 1.352941, 0.352941, 1.411765, 0.411765, 1.470588, 0.470588, 1.529412, 0.529412, 1.588235, 0.588235, 1.647059, 0.647059, 1.705882, 0.705882, 1.764706, 0.764706, 1.823529, 0.823529, 1.882353, 0.882353, 1.941176, 0.941176]
- added `SliceEncodingDirection`: "k"
- added `DwellTime`: 1.6000000000000001e-06
- added `RepetitionTime`: 2.0
- added `Modality`: "MR"
- added `ProtocolName`: "LFO_Relax_ offline"
- added `SeriesDescription`: "LFO_Relax_ offline"
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
- added `DerivedVendorReportedEchoSpacing`: 0.00032
- added `SpacingBetweenSlices`: 3.0
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "Allegra"
- removed `FatSaturation`: true
- removed `AcquisitionMatrixSE`: 33
- removed `ReconMatrixSE`: 33
- removed `RepetitionTimeExcitation`: 2.0
- changed `EffectiveEchoSpacing`: 3.200204813108039e-06 -> 0.00032
- changed `EchoTime`: 0.015 -> 0.03
- changed `FlipAngle`: 90.0 -> 82.0
- changed `BandwidthPerPixelPhaseEncode`: "3906" -> 48.82812499999999
- changed `SliceThickness`: 4.0 -> 3.0

## NYU/2/dwi.json

- added `ManufacturersModelName`: "Allegra"
- added `SoftwareVersions`: "syngo MR 2004A"
- added `ReceiveCoilActiveElements`: "TR"
- added `ScanningSequence`: ["EP"]
- added `SequenceVariant`: ["SK", "SP"]
- added `ScanOptions`: ["FS"]
- added `SequenceName`: "cbi_ep2d_diff_1_4"
- added `PulseSequenceDetails`: "%CustomerSeq%\\cbi_ep2d_diff_1_4"
- added `MRAcquisitionType`: "2D"
- added `MTState`: false
- added `B0ShimmingTechnique`: "Standard"
- added `NumberShots`: 1
- added `PhaseEncodingDirection`: "i"
- added `TotalReadoutTime`: 0.02016
- added `SliceTiming`: [2.6, 0.0, 2.704, 0.104, 2.808, 0.208, 2.912, 0.312, 3.016, 0.416, 3.12, 0.52, 3.224, 0.624, 3.328, 0.728, 3.432, 0.832, 3.536, 0.936, 3.64, 1.04, 3.744, 1.144, 3.848, 1.248, 3.952, 1.352, 4.056, 1.456, 4.16, 1.56, 4.264, 1.664, 4.368, 1.768, 4.472, 1.872, 4.576, 1.976, 4.68, 2.08, 4.784, 2.184, 4.888, 2.288, 4.992, 2.392, 5.096, 2.496]
- added `DwellTime`: 2.1000000000000002e-06
- added `RepetitionTime`: 5.2
- added `Modality`: "MR"
- added `ProtocolName`: "dti_64dir_3x3x3"
- added `SeriesDescription`: "dti_64dir_3x3x3"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 64
- added `AcquisitionMatrixPE`: 64
- added `ReconMatrixPE`: 64
- added `PercentPhaseFOV`: 100.0
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 64
- added `EchoTrainLength`: 64
- added `PixelBandwidth`: 3720.0
- added `DerivedVendorReportedEchoSpacing`: 0.00032
- added `SpacingBetweenSlices`: 3.0
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "Allegra"
- removed `FlipAngle`: 60.0
- removed `FatSaturation`: false
- removed `AcquisitionMatrixSE`: 50
- removed `ReconMatrixSE`: 50
- removed `RepetitionTimeExcitation`: 5.2
- changed `EffectiveEchoSpacing`: 4.200268817204301e-06 -> 0.00032
- changed `SliceEncodingDirection`: "A" -> "k"
- changed `BandwidthPerPixelPhaseEncode`: "3720" -> 48.82812499999999

## OHSU/1/T1w.json

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
- added `RepetitionTimeExcitation`: 0.008199999999999999
- added `Modality`: "MR"
- added `ProtocolName`: "T1Anatomical-1"
- added `SeriesDescription`: "T1Anatomical-1"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 256
- added `AcquisitionMatrixPE`: 240
- added `ReconMatrixPE`: 240
- added `PercentPhaseFOV`: 93.8
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 240
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TriTim"
- removed `FatSaturation`: false
- removed `AcquisitionMatrixSE`: 160
- removed `ReconMatrixSE`: 160
- changed `PulseSequenceType`: "TFL" -> "MPRAGE"
- changed `SliceEncodingDirection`: "A" -> "i"
- changed `DwellTime`: 2.170138888888889e-05 -> 1.0851e-05
- changed `PixelBandwidth`: "180" -> 180.0

## OHSU/1/bold.json

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
- added `PhaseEncodingDirection`: "j-"
- added `TotalReadoutTime`: 0.0315
- added `SliceTiming`: [1.25, 0.0, 1.319444, 0.069444, 1.388889, 0.138889, 1.458333, 0.208333, 1.527778, 0.277778, 1.597222, 0.347222, 1.666667, 0.416667, 1.736111, 0.486111, 1.805556, 0.555556, 1.875, 0.625, 1.944444, 0.694444, 2.013889, 0.763889, 2.083333, 0.833333, 2.152778, 0.902778, 2.222222, 0.972222, 2.291667, 1.041667, 2.361111, 1.111111, 2.430556, 1.180556]
- added `SliceEncodingDirection`: "k"
- added `DwellTime`: 3.4e-06
- added `RepetitionTime`: 2.5
- added `Modality`: "MR"
- added `ProtocolName`: "REST1"
- added `SeriesDescription`: "REST1"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 64
- added `AcquisitionMatrixPE`: 64
- added `ReconMatrixPE`: 64
- added `PercentPhaseFOV`: 100.0
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 64
- added `EchoTrainLength`: 64
- added `PixelBandwidth`: 2298.0
- added `DerivedVendorReportedEchoSpacing`: 0.0005
- added `SpacingBetweenSlices`: 3.8
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TriTim"
- removed `FatSaturation`: true
- removed `AcquisitionMatrixSE`: 36
- removed `ReconMatrixSE`: 36
- removed `RepetitionTimeExcitation`: 2.5
- changed `EffectiveEchoSpacing`: 6.799390774586597e-06 -> 0.0005
- changed `BandwidthPerPixelPhaseEncode`: "2298" -> 31.25

## SDSU/1/T1w.json

- added `ReceiveCoilName`: "8HRBRAIN"
- added `Modality`: "MR"
- added `ProtocolName`: "FSPGR_Anat"
- added `SeriesDescription`: "FSPGR_Anat"
- added `ScanningSequence`: ["GR", "IR"]
- added `ParallelAcquisitionTechnique`: "ASSET"
- added `AcquisitionMatrixPE`: 192
- added `PercentPhaseFOV`: 100.0
- added `NumberOfAverages`: 1.0
- added `PhaseEncodingAxis`: "j"
- removed `ManufacturerModelName`: "MR750"
- removed `MagneticFieldStrength`: 3.0
- removed `EchoTime`: 0.0031720000000000003
- removed `SliceEncodingDirection`: "A"
- removed `FatSaturation`: false
- removed `AcquisitionMatrixSE`: 172
- removed `ReconMatrixSE`: 172
- removed `RepetitionTimePreparation`: 0.008136
- removed `PixelBandwidth`: "244.141"
- removed `DwellTime`: 1.599997542403775e-05
- changed `PulseSequenceType`: "SPGR" -> "Inversion Recovery Fast Spoiled Gradient Echo"

## SDSU/1/bold.json

- added `ReceiveCoilName`: "8HRBRAIN"
- added `Modality`: "MR"
- added `ProtocolName`: "EPI_REST"
- added `SeriesDescription`: "EPI_REST"
- added `RepetitionTime`: 2.0
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP", "GR"]
- added `AcquisitionMatrixPE`: 64
- added `PercentPhaseFOV`: 100.0
- added `NumberOfAverages`: 1.0
- added `SpacingBetweenSlices`: 3.4
- added `NumberShots`: 1
- added `PhaseEncodingAxis`: "j"
- added `SliceTiming`: [0.0, 1.0, 0.047619, 1.047619, 0.095238, 1.095238, 0.142857, 1.142857, 0.190476, 1.190476, 0.238095, 1.238095, 0.285714, 1.285714, 0.333333, 1.333333, 0.380952, 1.380952, 0.428571, 1.428571, 0.47619, 1.47619, 0.52381, 1.52381, 0.571429, 1.571429, 0.619048, 1.619048, 0.666667, 1.666667, 0.714286, 1.714286, 0.761905, 1.761905, 0.809524, 1.809524, 0.857143, 1.857143, 0.904762, 1.904762, 0.952381, 1.952381]
- removed `ManufacturerModelName`: "MR750"
- removed `MagneticFieldStrength`: 3.0
- removed `SliceEncodingDirection`: "A"
- removed `FatSaturation`: false
- removed `AcquisitionMatrixSE`: 42
- removed `ReconMatrixSE`: 42
- removed `RepetitionTimeExcitation`: 2.0
- removed `BandwidthPerPixelPhaseEncode`: "7812.5"
- removed `EffectiveEchoSpacing`: 2e-06

## SDSU/1/dwi.json

- added `ReceiveCoilName`: "8HRBRAIN"
- added `Modality`: "MR"
- added `ProtocolName`: "DTI"
- added `SeriesDescription`: "DTI"
- added `RepetitionTime`: 8.5
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP", "SE"]
- added `ParallelAcquisitionTechnique`: "ASSET"
- added `AcquisitionMatrixPE`: 128
- added `PercentPhaseFOV`: 100.0
- added `SpacingBetweenSlices`: 2.0
- added `NumberShots`: 1
- added `PhaseEncodingAxis`: "j"
- removed `ManufacturerModelName`: "MR750"
- removed `MagneticFieldStrength`: 3.0
- removed `SliceEncodingDirection`: "A"
- removed `FatSaturation`: true
- removed `AcquisitionMatrixSE`: 68
- removed `ReconMatrixSE`: 68
- removed `RepetitionTimeExcitation`: 8.5

## TCD/1/T1w.json

- added `ImagingFrequency`: 127.728
- added `Modality`: "MR"
- added `ProtocolName`: "T1w"
- added `ReceiveCoilName`: "SENSE-Head-8"
- added `RepetitionTime`: 0.008400000000000001
- added `ScanningSequence`: ["GR", "IR"]
- added `RepetitionTimeExcitation`: 0.008400000000000001
- added `ParallelReductionFactorOutOfPlane`: 1.0
- added `AcquisitionMatrixPE`: 256
- added `ReconMatrixPE`: 256
- added `PhaseEncodingAxis`: "i"
- removed `ManufacturerModelName`: "Achieva"
- removed `SliceEncodingDirection`: "A"
- removed `SliceThickness`: 0.9
- removed `FatSaturation`: false
- removed `AcquisitionMatrixSE`: 190
- removed `ReconMatrixSE`: 190
- removed `PixelBandwidth`: "188.3"
- removed `DwellTime`: 2.0744822092405733e-05
- changed `RepetitionTimePreparation`: 0.008400000000000001 -> 3.0
- changed `PulseSequenceType`: "MPRAGE" -> "Inversion Recovery Turbo Field Echo (MPRAGE-like)"

## TCD/1/bold.json

- added `ImagingFrequency`: 127.728
- added `Modality`: "MR"
- added `ProtocolName`: "bold"
- added `ReceiveCoilName`: "SENSE-Head-8"
- added `RepetitionTime`: 2.0
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP", "GR"]
- added `ParallelReductionFactorInPlane`: 2.0
- added `AcquisitionMatrixPE`: 80
- added `ReconMatrixPE`: 80
- added `SpacingBetweenSlices`: 3.55
- added `PhaseEncodingAxis`: "j"
- added `WaterFatShift`: 11.0
- added `EchoTrainLength`: 43
- added `EstimatedTotalReadoutTime`: 0.0247538887783599
- added `EstimatedEffectiveEchoSpacing`: 0.0003133403642830367
- removed `ManufacturerModelName`: "Achieva"
- removed `SliceEncodingDirection`: "A"
- removed `FatSaturation`: true
- removed `AcquisitionMatrixSE`: 37
- removed `ReconMatrixSE`: 37
- removed `RepetitionTimeExcitation`: 2.0
- removed `BandwidthPerPixelPhaseEncode`: "39.5/pixel"
- changed `SliceThickness`: 3.2 -> 3.20000005

## TCD/1/dwi.json

- added `Modality`: "MR"
- added `ProtocolName`: "dwi"
- added `ReceiveCoilName`: "SENSE-Head-8"
- added `MRAcquisitionType`: "2D"
- added `ScanningSequence`: ["EP", "SE"]
- added `ParallelAcquisitionTechnique`: "SENSE"
- added `ParallelReductionFactorInPlane`: 2.0
- added `AcquisitionMatrixPE`: 124
- added `ReconMatrixPE`: 128
- added `SpacingBetweenSlices`: 2.0
- added `PhaseEncodingAxis`: "j"
- removed `ManufacturerModelName`: "Achieva"
- removed `MagneticFieldStrength`: 3.0
- removed `EchoTime`: 0.079
- removed `SliceEncodingDirection`: "A"
- removed `AcquisitionMatrixSE`: 65
- removed `ReconMatrixSE`: 65
- removed `RepetitionTimeExcitation`: 20.244
- removed `BandwidthPerPixelPhaseEncode`: "2590.6"
- removed `EffectiveEchoSpacing`: 3.1129916347688794e-06

## UCD/1/T1w.json

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
- added `RepetitionTime`: 2.0
- added `RepetitionTimeExcitation`: 0.0075
- added `Modality`: "MR"
- added `ProtocolName`: "mprage_sag_NS"
- added `SeriesDescription`: "mprage_sag_NS"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 256
- added `AcquisitionMatrixPE`: 224
- added `ReconMatrixPE`: 224
- added `PercentPhaseFOV`: 87.5
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 224
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TriTim"
- removed `FatSaturation`: false
- removed `AcquisitionMatrixSE`: 192
- removed `ReconMatrixSE`: 192
- changed `SliceEncodingDirection`: "A" -> "i-"
- changed `DwellTime`: 1.7755681818181817e-05 -> 8.878e-06
- changed `PixelBandwidth`: "220" -> 220.0

## UCD/1/bold.json

- added `ManufacturersModelName`: "TrioTim"
- added `SoftwareVersions`: "syngo MR B17"
- added `ReceiveCoilActiveElements`: "HEA;HEP"
- added `MatrixCoilMode`: "Auto (CP)"
- added `CoilCombinationMethod`: "Adaptive Combine"
- added `ScanningSequence`: ["EP"]
- added `SequenceVariant`: ["SK"]
- added `ScanOptions`: ["FS"]
- added `SequenceName`: "ep2d_bold"
- added `PulseSequenceDetails`: "%SiemensSeq%\\ep2d_bold"
- added `MRAcquisitionType`: "2D"
- added `MTState`: false
- added `B0ShimmingTechnique`: "Advanced"
- added `NumberShots`: 1
- added `PhaseEncodingDirection`: "j-"
- added `TotalReadoutTime`: 0.032130000000000006
- added `SliceTiming`: [1.0, 0.0, 1.055556, 0.055556, 1.111111, 0.111111, 1.166667, 0.166667, 1.222222, 0.222222, 1.277778, 0.277778, 1.333333, 0.333333, 1.388889, 0.388889, 1.444444, 0.444444, 1.5, 0.5, 1.555556, 0.555556, 1.611111, 0.611111, 1.666667, 0.666667, 1.722222, 0.722222, 1.777778, 0.777778, 1.833333, 0.833333, 1.888889, 0.888889, 1.944444, 0.944444]
- added `SliceEncodingDirection`: "k"
- added `DwellTime`: 3.5000000000000004e-06
- added `RepetitionTime`: 2.0
- added `Modality`: "MR"
- added `ProtocolName`: "func_inplanes"
- added `SeriesDescription`: "func_inplanes"
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
- added `DerivedVendorReportedEchoSpacing`: 0.00051
- added `SpacingBetweenSlices`: 3.5
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TriTim"
- removed `FatSaturation`: true
- removed `AcquisitionMatrixSE`: 36
- removed `ReconMatrixSE`: 36
- removed `RepetitionTimeExcitation`: 2.0
- changed `EffectiveEchoSpacing`: 7.000448028673835e-06 -> 0.00051
- changed `BandwidthPerPixelPhaseEncode`: "2232" -> 30.637254901960784
- changed `SliceThickness`: 4.0 -> 3.5

## UCLA/1/T1w.json

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
- added `RepetitionTimeExcitation`: 0.0067
- added `Modality`: "MR"
- added `ProtocolName`: "ADNI_MPRAGE"
- added `SeriesDescription`: "ADNI_MPRAGE"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 256
- added `AcquisitionMatrixPE`: 240
- added `ReconMatrixPE`: 240
- added `PercentPhaseFOV`: 93.8
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 240
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TriTim"
- removed `FatSaturation`: false
- removed `AcquisitionMatrixSE`: 160
- removed `ReconMatrixSE`: 160
- changed `SliceEncodingDirection`: "A" -> "i"
- changed `DwellTime`: 1.6276041666666666e-05 -> 8.138e-06
- changed `PixelBandwidth`: "240" -> 240.0

## UCLA/1/bold.json

- added `ManufacturersModelName`: "TrioTim"
- added `SoftwareVersions`: "syngo MR B17"
- added `ReceiveCoilActiveElements`: "HEA;HEP"
- added `MatrixCoilMode`: "Auto (CP)"
- added `CoilCombinationMethod`: "rSOS"
- added `ScanningSequence`: ["EP"]
- added `SequenceVariant`: ["SK", "OSP"]
- added `ScanOptions`: ["FS"]
- added `SequenceName`: "ep2d_bold"
- added `PulseSequenceDetails`: "%SiemensSeq%\\ep2d_bold"
- added `MRAcquisitionType`: "2D"
- added `MTState`: false
- added `B0ShimmingTechnique`: "Standard"
- added `NumberShots`: 1
- added `PhaseEncodingDirection`: "j-"
- added `TotalReadoutTime`: 0.0322749
- added `SliceTiming`: [1.5, 0.0, 1.588235, 0.088235, 1.676471, 0.176471, 1.764706, 0.264706, 1.852941, 0.352941, 1.941176, 0.441176, 2.029412, 0.529412, 2.117647, 0.617647, 2.205882, 0.705882, 2.294118, 0.794118, 2.382353, 0.882353, 2.470588, 0.970588, 2.558824, 1.058824, 2.647059, 1.147059, 2.735294, 1.235294, 2.823529, 1.323529, 2.911765, 1.411765]
- added `SliceEncodingDirection`: "k"
- added `DwellTime`: 3.199e-06
- added `RepetitionTime`: 3.0
- added `Modality`: "MR"
- added `ProtocolName`: "Resting State 6 min- Dapretto TR=3s, TE=28ms, 3x3x4mm"
- added `SeriesDescription`: "Resting State 6 min- Dapretto TR=3s, TE=28ms, 3x3x4mm"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 64
- added `AcquisitionMatrixPE`: 64
- added `ReconMatrixPE`: 64
- added `PercentPhaseFOV`: 100.0
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 70
- added `EchoTrainLength`: 70
- added `PixelBandwidth`: 2442.0
- added `DerivedVendorReportedEchoSpacing`: 0.00047
- added `SpacingBetweenSlices`: 4.0
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TriTim"
- removed `FatSaturation`: true
- removed `AcquisitionMatrixSE`: 34
- removed `ReconMatrixSE`: 34
- removed `RepetitionTimeExcitation`: 3.0
- changed `EffectiveEchoSpacing`: 6.398443898443899e-06 -> 0.0005123
- changed `BandwidthPerPixelPhaseEncode`: "2442" -> 30.49970720281085

## UCLA/2/T1w.json

manual (no printout): kept as is

## UCLA/2/bold.json

manual (no printout): kept as is

## UCLA/Long/T1w.json

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
- added `RepetitionTimeExcitation`: 0.0067
- added `Modality`: "MR"
- added `ProtocolName`: "ADNI_MPRAGE"
- added `SeriesDescription`: "ADNI_MPRAGE"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 256
- added `AcquisitionMatrixPE`: 240
- added `ReconMatrixPE`: 240
- added `PercentPhaseFOV`: 93.8
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 240
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TriTim"
- removed `FatSaturation`: false
- removed `AcquisitionMatrixSE`: 160
- removed `ReconMatrixSE`: 160
- changed `SliceEncodingDirection`: "A" -> "i"
- changed `DwellTime`: 1.6276041666666666e-05 -> 8.138e-06
- changed `PixelBandwidth`: "240" -> 240.0

## UCLA/Long/bold.json

- added `ManufacturersModelName`: "TrioTim"
- added `SoftwareVersions`: "syngo MR B17"
- added `ReceiveCoilActiveElements`: "HEA;HEP"
- added `MatrixCoilMode`: "Auto (CP)"
- added `CoilCombinationMethod`: "rSOS"
- added `ScanningSequence`: ["EP"]
- added `SequenceVariant`: ["SK", "OSP"]
- added `ScanOptions`: ["FS"]
- added `SequenceName`: "ep2d_bold"
- added `PulseSequenceDetails`: "%SiemensSeq%\\ep2d_bold"
- added `MRAcquisitionType`: "2D"
- added `MTState`: false
- added `B0ShimmingTechnique`: "Standard"
- added `NumberShots`: 1
- added `PhaseEncodingDirection`: "j-"
- added `TotalReadoutTime`: 0.0322749
- added `SliceTiming`: [1.5, 0.0, 1.588235, 0.088235, 1.676471, 0.176471, 1.764706, 0.264706, 1.852941, 0.352941, 1.941176, 0.441176, 2.029412, 0.529412, 2.117647, 0.617647, 2.205882, 0.705882, 2.294118, 0.794118, 2.382353, 0.882353, 2.470588, 0.970588, 2.558824, 1.058824, 2.647059, 1.147059, 2.735294, 1.235294, 2.823529, 1.323529, 2.911765, 1.411765]
- added `SliceEncodingDirection`: "k"
- added `DwellTime`: 3.199e-06
- added `RepetitionTime`: 3.0
- added `Modality`: "MR"
- added `ProtocolName`: "Resting State 6 min- Dapretto TR=3s, TE=28ms, 3x3x4mm"
- added `SeriesDescription`: "Resting State 6 min- Dapretto TR=3s, TE=28ms, 3x3x4mm"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 64
- added `AcquisitionMatrixPE`: 64
- added `ReconMatrixPE`: 64
- added `PercentPhaseFOV`: 100.0
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 70
- added `EchoTrainLength`: 70
- added `PixelBandwidth`: 2442.0
- added `DerivedVendorReportedEchoSpacing`: 0.00047
- added `SpacingBetweenSlices`: 4.0
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TriTim"
- removed `FatSaturation`: true
- removed `AcquisitionMatrixSE`: 34
- removed `ReconMatrixSE`: 34
- removed `RepetitionTimeExcitation`: 3.0
- changed `EffectiveEchoSpacing`: 6.398443898443899e-06 -> 0.0005123
- changed `BandwidthPerPixelPhaseEncode`: "2442" -> 30.49970720281085

## UPSM/1/T1w.json

manual (no printout): kept as is

## UPSM/1/bold.json

manual (no printout): kept as is

## UPSM/Long/T1w.json

- added `ManufacturersModelName`: "Allegra"
- added `SoftwareVersions`: "syngo MR A30"
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
- added `RepetitionTime`: 2.1
- added `RepetitionTimeExcitation`: 0.0094
- added `Modality`: "MR"
- added `ProtocolName`: "TC_t1_mprage_sag_ns"
- added `SeriesDescription`: "TC_t1_mprage_sag_ns"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 256
- added `AcquisitionMatrixPE`: 256
- added `ReconMatrixPE`: 256
- added `PercentPhaseFOV`: 100.0
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 256
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "Allegra"
- removed `FatSaturation`: false
- removed `AcquisitionMatrixSE`: 176
- removed `ReconMatrixSE`: 176
- changed `SliceEncodingDirection`: "A" -> "i"
- changed `DwellTime`: 3.0048076923076925e-05 -> 1.5024e-05
- changed `PixelBandwidth`: "130" -> 130.0

## UPSM/Long/bold.json

- added `ManufacturersModelName`: "Allegra"
- added `SoftwareVersions`: "syngo MR A30"
- added `ReceiveCoilActiveElements`: "HE"
- added `ScanningSequence`: ["EP"]
- added `SequenceVariant`: ["SK"]
- added `ScanOptions`: ["FS"]
- added `SequenceName`: "ep2d_bold"
- added `PulseSequenceDetails`: "%SiemensSeq%\\ep2d_bold"
- added `MRAcquisitionType`: "2D"
- added `MTState`: false
- added `B0ShimmingTechnique`: "Standard"
- added `NumberShots`: 1
- added `PhaseEncodingDirection`: "j-"
- added `TotalReadoutTime`: 0.02331
- added `SliceTiming`: [0.0, 0.775862, 0.051724, 0.827586, 0.103448, 0.87931, 0.155172, 0.931034, 0.206897, 0.982759, 0.258621, 1.034483, 0.310345, 1.086207, 0.362069, 1.137931, 0.413793, 1.189655, 0.465517, 1.241379, 0.517241, 1.293103, 0.568966, 1.344828, 0.62069, 1.396552, 0.672414, 1.448276, 0.724138]
- added `SliceEncodingDirection`: "k"
- added `DwellTime`: 2.499e-06
- added `RepetitionTime`: 1.5
- added `Modality`: "MR"
- added `ProtocolName`: "ep2d_bold_rest"
- added `SeriesDescription`: "ep2d_bold_rest"
- added `ImageType`: ["ORIGINAL", "PRIMARY"]
- added `BaseResolution`: 64
- added `AcquisitionMatrixPE`: 64
- added `ReconMatrixPE`: 64
- added `PercentPhaseFOV`: 100.0
- added `PercentSampling`: 100.0
- added `PhaseResolution`: 1.0
- added `PhaseEncodingSteps`: 64
- added `EchoTrainLength`: 64
- added `PixelBandwidth`: 3126.0
- added `DerivedVendorReportedEchoSpacing`: 0.00037
- added `SpacingBetweenSlices`: 4.0
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "Allegra"
- removed `FatSaturation`: true
- removed `AcquisitionMatrixSE`: 29
- removed `ReconMatrixSE`: 29
- removed `RepetitionTimeExcitation`: 1.5
- changed `EffectiveEchoSpacing`: 4.998400511836212e-06 -> 0.00037
- changed `BandwidthPerPixelPhaseEncode`: "3126" -> 42.22972972972973

## USM/1/T1w.json

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
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TriTim"
- removed `FatSaturation`: false
- removed `AcquisitionMatrixSE`: 160
- removed `ReconMatrixSE`: 160
- changed `SliceEncodingDirection`: "A" -> "i"
- changed `DwellTime`: 1.6276041666666666e-05 -> 8.138e-06
- changed `RepetitionTimePreparation`: 0.9 -> 2.3000000000000003
- changed `PixelBandwidth`: "240" -> 240.0

## USM/1/bold.json

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
- added `PhaseEncodingDirection`: "j-"
- added `TotalReadoutTime`: 0.013545
- added `SliceTiming`: [1.0, 0.0, 1.05, 0.05, 1.1, 0.1, 1.15, 0.15, 1.2, 0.2, 1.25, 0.25, 1.3, 0.3, 1.35, 0.35, 1.4, 0.4, 1.45, 0.45, 1.5, 0.5, 1.55, 0.55, 1.6, 0.6, 1.65, 0.65, 1.7, 0.7, 1.75, 0.75, 1.8, 0.8, 1.85, 0.85, 1.9, 0.9, 1.95, 0.95]
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
- added `DerivedVendorReportedEchoSpacing`: 0.00043
- added `SpacingBetweenSlices`: 3.3
- added `NumberOfAverages`: 1
- removed `ManufacturerModelName`: "TriTim"
- removed `FatSaturation`: true
- removed `AcquisitionMatrixSE`: 40
- removed `ReconMatrixSE`: 40
- removed `RepetitionTimeExcitation`: 2.0
- changed `EffectiveEchoSpacing`: 5.399101589495508e-06 -> 0.000215
- changed `SliceEncodingDirection`: "A" -> "k"
- changed `BandwidthPerPixelPhaseEncode`: "2894" -> 72.67441860465117
