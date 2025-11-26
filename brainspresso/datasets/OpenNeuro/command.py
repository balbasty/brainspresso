from cyclopts import App
from brainspresso.cli import app

openneuro_help = """
Commands related to datasets hosted on OpenNeuro
"""  # noqa: E501

app.command(openneuro := App(name="openneuro", help=openneuro_help))
