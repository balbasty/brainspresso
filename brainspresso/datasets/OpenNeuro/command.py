from cyclopts import App
from brainspresso.cli import app

openneuro_help = """
Any OpenNeuro dataset (BIDS)

* **Host**          https://openneuro.org (GraphQL API) and s3://openneuro.org
* **Format**        Already BIDS: harvest downloads a filtered subset
* **License**       Per dataset (mostly CC0)
"""  # noqa: E501

app.command(
    openneuro := App(name="openneuro", help=openneuro_help)
)
