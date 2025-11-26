from cyclopts import App
from braindataprep.cli import app

adhd200_help = """
Commands related to the ADHD-200 dataset

* **Project**       ADHD-200
* **Modalities**    T1w, rest
* **Populations**   ADHD
* **License**       CC BY-NC
* **Funding**       See dataset.json
* **URL**           https://fcon_1000.projects.nitrc.org/indi/adhd200/
"""

app.command(adhd200 := App(name="adhd200", help=adhd200_help))
