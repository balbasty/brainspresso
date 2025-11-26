from cyclopts import App
from braindataprep.cli import app

adni1_help = """
Commands related to the CoRR dataset

* **Project**       Alzheimer's Disease Neuroimaging Initiative (ADNI)
* **Subproject**    ADNI-1
* **Modalities**    T1w, DTI, PET
* **Populations**   Controls, Alzheimer
* **URL**           https://adni.loni.usc.edu
"""  # noqa: E501

app.command(adni1 := App(name="adni1", help=adni1_help))
