from cyclopts import App
from braindataprep.cli import app

abide1_help = """
Commands related to the ABIDE-I dataset

* **Project**       Autism Brain Imaging Data Exchange (ABIDE)
* **Subproject**    ABIDE I (original 17 sites)
* **Modalities**    T1w, rest
* **Populations**   Controls, Autism Spectrum Disorders
* **License**       CC BY-NC
* **Funding**       NIH: K23 MH087770, R03 MH096321, and more.
* **URL**           https://fcon_1000.projects.nitrc.org/indi/abide/abide_I.html
"""  # noqa: E501

app.command(abide1 := App(name="abide1", help=abide1_help))
