from cyclopts import App
from braindataprep.cli import app

gsp_help = """
Commands related to the GSP dataset

* **Project**       Brain Genomics Superstruct Project (CoRR)
* **Modalities**    T1w, rest
* **Populations**   Controls
* **URL**           https://doi.org/10.7910/DVN/25833
"""  # noqa: E501

app.command(gsp := App(name="gsp", help=gsp_help))
