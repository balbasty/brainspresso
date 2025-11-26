from cyclopts import App
from braindataprep.cli import app

abide2_help = """
Commands related to the ABIDE-II dataset

* **Project**       Autism Brain Imaging Data Exchange (ABIDE)
* **Subproject**    ABIDE II (additional 19 sites)
* **Modalities**    T1w, rest, (DWI, FLAIR)
* **Populations**   Controls, Autism Spectrum Disorders
* **License**       CC BY-NC
* **Funding**       NIH: R21 MH107045, and more.
* **URL**           https://fcon_1000.projects.nitrc.org/indi/abide/abide_II.html
"""  # noqa: E501

app.command(abide2 := App(name="abide2", help=abide2_help))
