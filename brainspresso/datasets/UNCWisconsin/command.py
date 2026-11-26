from cyclopts import App
from brainspresso.cli import app
from brainspresso.datasets.compost import add_compost

uncwisc_help = """
UNC-Wisconsin Rhesus Macaque Neurodevelopment Database

* **Species**       Rhesus macaque (Macaca mulatta)
* **Modalities**    T1w, T2w, DWI (NRRD, converted to NIfTI)
* **Populations**   Healthy, longitudinal (a few weeks to 3 years)
* **Host**          https://data.kitware.com (Girder), no login needed
* **Citation**      https://doi.org/10.3389/fnins.2017.00029
"""

app.command(uncwisc := App(name="unc-wisconsin", help=uncwisc_help))
add_compost(uncwisc, "UNC-Wisconsin")
