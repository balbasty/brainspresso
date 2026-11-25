from brainspresso.utils.deps import requires

# The `adhd200` command is only registered if one of its subcommands
# is available.
if requires("adhd200 harvest", "fsspec", "s3fs", extra="adhd200"):
    from .command import adhd200 as _adhd200        # noqa: F401
    from .download import download as _download     # noqa: F401
    # from .bidsify import bidsify as _bidsify      # noqa: F401
