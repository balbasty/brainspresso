## Suspected protocol2bids problems

Collected automatically by comparing the regenerated sidecars with the
previous (hand-parsed) templates. Values were NOT hand-fixed: fix
protocol2bids (or add `defaults`/`assigns` in `sidecars.toml`) and re-run.

### `ManufacturerModelName` dropped (34)

- `CMU/T1w.json` <- `CMU_b/anat.pdf`: was "Verio"
- `CMU/bold.json` <- `CMU_b/rest.pdf` (select `1`): was "Verio"
- `Caltech/T1w.json` <- `Caltech/anat.pdf`: was "TrioTim"
- `Caltech/bold.json` <- `Caltech/rest.pdf`: was "TrioTim"
- `KKI/T1w.json` <- `KKI/anat.pdf`: was "Achieva"
- `KKI/bold.json` <- `KKI/rest.pdf`: was "Achieva"
- `Leuven/T1w.json` <- `Leuven_1/anat.pdf`: was "Intera"
- `Leuven/bold.json` <- `Leuven_1/rest.pdf`: was "Intera"
- `MaxMun/T1w.json` <- `MaxMun_a/anat.pdf`: was "Verio"
- `MaxMun/bold.json` <- `MaxMun_a/rest.pdf` (select `0`): was "Verio"
- `NYU/T1w.json` <- `NYU/anat.pdf`: was "Allegra"
- `NYU/bold.json` <- `NYU/rest.pdf`: was "Allegra"
- `OHSU/T1w.json` <- `OHSU/anat.pdf`: was "TrioTim"
- `OHSU/bold.json` <- `OHSU/rest.pdf`: was "TrioTim"
- `Olin/T1w.json` <- `Olin/anat.pdf`: was "Allegra"
- `Olin/bold.json` <- `Olin/rest.pdf`: was "Verio"
- `Pitt/T1w.json` <- `Pitt/anat.pdf`: was "Allegra"
- `Pitt/bold.json` <- `Pitt/rest.pdf`: was "Allegra"
- `SBL/T1w.json` <- `SBL/anat.pdf`: was "Intera"
- `SBL/bold.json` <- `SBL/rest.pdf`: was "Intera"
- `SDSU/T1w.json` <- `SDSU/anat.pdf`: was "MR750"
- `SDSU/bold.json` <- `SDSU/rest.pdf`: was "MR750"
- `Stanford/T1w.json` <- `Stanford/anat.pdf`: was "Signa"
- `Stanford/bold.json` <- `Stanford/rest.pdf`: was "Signa"
- `Trinity/T1w.json` <- `Trinity/anat.pdf`: was "Achieva"
- `Trinity/bold.json` <- `Trinity/rest.pdf`: was "Achieva"
- `UCLA/T1w.json` <- `UCLA_1/anat.pdf` (select `0`): was "TrioTim"
- `UCLA/bold.json` <- `UCLA_1/rest.pdf`: was "TrioTim"
- `UM/T1w.json` <- `UM_1/anat.pdf` (select `1`): was "Signa"
- `UM/bold.json` <- `UM_1/rest.pdf`: was "Signa"
- `USM/T1w.json` <- `USM/anat.pdf`: was "TrioTim"
- `USM/bold.json` <- `USM/rest.pdf`: was "TrioTim"
- `Yale/T1w.json` <- `Yale/anat.pdf` (select `1`): was "TrioTim"
- `Yale/bold.json` <- `Yale/rest.pdf`: was "TrioTim"

### `RepetitionTimeExcitation` dropped (18)

- `CMU/bold.json` <- `CMU_b/rest.pdf` (select `1`): was 2
- `Caltech/bold.json` <- `Caltech/rest.pdf`: was 2
- `KKI/bold.json` <- `KKI/rest.pdf`: was 2.5
- `Leuven/bold.json` <- `Leuven_1/rest.pdf`: was 1.667
- `MaxMun/bold.json` <- `MaxMun_a/rest.pdf` (select `0`): was 3
- `NYU/bold.json` <- `NYU/rest.pdf`: was 3
- `OHSU/bold.json` <- `OHSU/rest.pdf`: was 2.5
- `Olin/bold.json` <- `Olin/rest.pdf`: was 1.5
- `Pitt/bold.json` <- `Pitt/rest.pdf`: was 1.5
- `SBL/bold.json` <- `SBL/rest.pdf`: was 2.2
- `SDSU/bold.json` <- `SDSU/rest.pdf`: was 2
- `Stanford/T1w.json` <- `Stanford/anat.pdf`: was 0.0084
- `Stanford/bold.json` <- `Stanford/rest.pdf`: was 2
- `Trinity/bold.json` <- `Trinity/rest.pdf`: was 2
- `UCLA/bold.json` <- `UCLA_1/rest.pdf`: was 3
- `UM/bold.json` <- `UM_1/rest.pdf`: was 2
- `USM/bold.json` <- `USM/rest.pdf`: was 2
- `Yale/bold.json` <- `Yale/rest.pdf`: was 2

### `MagneticFieldStrength` dropped (5)

- `SDSU/T1w.json` <- `SDSU/anat.pdf`: was 3
- `SDSU/bold.json` <- `SDSU/rest.pdf`: was 3
- `Stanford/bold.json` <- `Stanford/rest.pdf`: was 3
- `UM/T1w.json` <- `UM_1/anat.pdf` (select `1`): was 3
- `UM/bold.json` <- `UM_1/rest.pdf`: was 3

### `EchoTime` differs (2)

- `Leuven/T1w.json` <- `Leuven_1/anat.pdf`: 0.00460331869 -> 0.0046
- `Yale/T1w.json` <- `Yale/anat.pdf` (select `1`): 0.00173 -> 0.00296

### no repetition time produced (2)

- `SDSU/T1w.json` <- `SDSU/anat.pdf`
- `UM/T1w.json` <- `UM_1/anat.pdf` (select `1`)

### `MRAcquisitionType` dropped (2)

- `Stanford/T1w.json` <- `Stanford/anat.pdf`: was "3D"
- `Stanford/bold.json` <- `Stanford/rest.pdf`: was "2D"

### `FlipAngle` differs (1)

- `NYU/bold.json` <- `NYU/rest.pdf`: 80 -> 90.0

### `InversionTime` dropped (1)

- `Trinity/T1w.json` <- `Trinity/anat.pdf`: was 1060.17468

### `InversionTime` differs (1)

- `Yale/T1w.json` <- `Yale/anat.pdf` (select `1`): 0.624 -> 0.9

## Non-BIDS keys dropped from the old templates (not re-added)

None.
