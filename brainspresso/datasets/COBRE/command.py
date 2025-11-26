from cyclopts import App
from brainspresso.cli import app

cobre_help = """
Commands related to the COBRE dataset

* **Project**       Center for Biomedical Research Excellence (COBRE)
* **Modalities**    T1w, rest
* **Populations**   Controls, Schizophrenia
* **License**       CC BY-NC
* **Funding**       NIH 1P20 RR021938-01A2
* **URL**           https://fcon_1000.projects.nitrc.org/indi/retro/cobre.html
"""

app.command(cobre := App(name="cobre", help=cobre_help))
