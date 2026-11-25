from cyclopts import App
from brainspresso.cli import app

bullitt_help = """
Designed Database of MR Brain Images of Healthy Volunteers (Bullitt, UNC)

* **Modalities**    T1w (FLASH, MPRAGE), T2w, MRA (DTI: harvest only)
* **Populations**   100+ healthy adults, 18 to 72 years
* **Host**          https://data.kitware.com (Girder), no login needed
"""

app.command(bullitt := App(name="bullitt", help=bullitt_help))
