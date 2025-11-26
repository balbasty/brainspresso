import cyclopts

_help = """
brainspresso : Download, Bidsify and Process public datasets
=============================================================
"""

main = app = cyclopts.App("brainspresso", help=_help, help_format="markdown")

# def main(*a, **k):
#     return app(exit_on_error=False)

if __name__ == "__main__":
    main()
