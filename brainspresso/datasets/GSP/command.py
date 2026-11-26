from cyclopts import App
from brainspresso.cli import app
from brainspresso.datasets.compost import add_compost

gsp_help = """
GSP dataset (controls, genomics)

* **Project**       Brain Genomics Superstruct Project (CoRR)
* **Modalities**    T1w, rest
* **Populations**   Controls
* **URL**           https://doi.org/10.7910/DVN/25833
"""  # noqa: E501

app.command(gsp := App(name="gsp", help=gsp_help))

add_compost(gsp, "GSP")
