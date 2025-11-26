import cyclopts

_help = """
Brainspresso : Harvest, Roast and Grind public neuroimaging datasets
====================================================================

* Plantation : A public neuroimaging dataset supported by brainspresso
* Harvest    : Download raw data
* Roast      : Bidsify raw data
* Grind      : Preprocess data with standard pipelines
"""

main = app = cyclopts.App(
    "brainspresso",
    help=_help,
    help_format="markdown",
    group_commands="Plantations",
    group_parameters="Options"
)
app._commands["--help"].group = "Help"
app._commands["--version"].group = "Help"

# def main(*a, **k):
#     return app(exit_on_error=False)

if __name__ == "__main__":
    main()
