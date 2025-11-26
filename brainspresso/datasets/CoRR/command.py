from cyclopts import App
from brainspresso.cli import app

corr_help = """
Commands related to the CoRR dataset

* **Project**       Consortium for Reliability and Reproducibility (CoRR)
* **Modalities**    T1w, rest, DWI, CBF, ASL
* **Populations**   Controls
* **URL**           https://fcon_1000.projects.nitrc.org/indi/CoRR/html/index.html
"""  # noqa: E501

app.command(corr := App(name="corr", help=corr_help))
