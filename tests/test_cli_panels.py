"""Help panels of the dataset commands (cyclopts groups, not rendered help)."""
import pytest

from brainspresso.datasets.catalog import CATALOG, load
from brainspresso.datasets.options import (
    HarvestOptions,
    Panel,
    source_panels,
)
from brainspresso.datasets.register import (
    command_names,
    config_options,
    register,
)


def panels(name: str, command: str) -> dict[str, set[str]]:
    """Panel title -> the flags it shows (hidden aliases left out)."""
    app = register(load(name))[command]
    out = {}
    for arg in app.assemble_argument_collection():
        if arg.children or arg.parameter.show is False:
            continue            # (a flattened group, a hidden alias)
        for group in arg.parameter.group:
            out.setdefault(group.name, set()).add(arg.name)
    return out


def sort_keys(name: str, command: str) -> dict[str, float]:
    """Panel title -> sort key."""
    app = register(load(name))[command]
    return {group.name: group.sort_key
            for arg in app.assemble_argument_collection()
            for group in arg.parameter.group}


def test_abide1_harvest_source_panels():
    found = panels("abide1", "harvest")
    login = {"--user", "--password"}
    assert found["NITRC (--source nitrc)"] == login
    assert found["XNAT + NITRC (--source xnat)"] == login
    # s3 reads no option of its own: no panel; nor OpenNeuro's
    assert not any(t.startswith(("S3", "OpenNeuro")) for t in found)
    assert "Credentials" not in found
    assert found["Sources"] == {"--source", "--jobs", "--packet",
                                "--insecure"}
    assert found["Dataset"] == {"--path", "--out", "--layout"}


def test_ixi_harvest_has_no_s3_panel():
    found = panels("ixi", "harvest")
    assert not any(t.startswith("S3") for t in found)
    assert found["NITRC-IR (--source nitrc)"] == {"--user", "--password"}


def test_single_source_panel_without_source_flag():
    # one source: its panel is not "(--source ...)"
    assert panels("gsp", "harvest")["Dataverse"] == {"--token"}
    assert panels("oasis3", "harvest")["XNAT"] == {"--user", "--password"}


def test_dataset_panels():
    assert panels("oasis1", "harvest")["OASIS-1"] == {"--discs"}
    assert panels("oasis1", "roast")["OASIS-1"] == {"--discs"}
    assert panels("adhd200", "harvest")["ADHD-200"] == {"--bids"}
    found = panels("openneuro", "harvest")
    assert found["OpenNeuro"] == {"--tag", "--source", "--token"}
    assert {"--dataset", "--path", "--out"} <= found["Dataset"]
    assert panels("openneuro", "info")["OpenNeuro"] == {"--source",
                                                        "--token"}
    # a collection: INDI projects
    assert panels("hbn", "info")["INDI"] == {"--derivatives"}


def test_hosted_options_panels():
    found = panels("hbn", "harvest")
    assert "--folders" in found["Filters"]
    assert {"--trim", "--list", "--sidecars", "--repair"} <= found["Harvest"]
    assert "--repair" in panels("hbn", "roast")["Roast"]
    assert "Metadata" not in panels("openneuro", "roast")


@pytest.mark.parametrize("name", sorted(CATALOG))
def test_panel_order(name):
    order = ["Dataset", "Selection", "Filters", "Keys", "Harvest", "Roast",
             "Compost", "Sources", "Logging"]
    ds = load(name)
    for command in command_names(ds):
        keys = sort_keys(name, command)
        known = [keys[t] for t in order if t in keys]
        assert known == sorted(known), (command, keys)
        for title, key in keys.items():
            if "(--source" in title:
                assert Panel.SOURCE <= key < Panel.COLLECTION
        if "Logging" in keys:
            assert keys["Logging"] == max(keys.values())


def test_every_option_has_a_panel():
    for name in CATALOG:
        ds = load(name)
        for command in command_names(ds):
            assert "Parameters" not in panels(name, command), (name, command)
            assert "Options" not in panels(name, command), (name, command)


def test_source_panels_follow_sources():
    ds = load("abide1")
    assert [g.name for g in source_panels(ds, ["user"])] == [
        "NITRC (--source nitrc)", "XNAT + NITRC (--source xnat)"]
    assert source_panels(ds, ["token"]) == ()


def test_key_choices_in_declared_order():
    choices = config_options(load("abide1"), "harvest")["keys"].choices
    assert choices[:6] == ("all", "meta", "pheno", "raw", "mri", "anat")
    assert choices == load("abide1").keys.all_choices()
    # the order of the hierarchy listed in the help
    tree = load("abide1").key_help("harvest")
    listed = [line.strip()[2:] for line in tree.splitlines()]
    assert list(choices) == listed
    # explicit choices keep their order
    assert config_options(load("oasis1"), "harvest")["keys"].choices == (
        "raw", "fs", "meta", "reliability", "facts")


def test_help_of_shared_source_options():
    found = config_options(load("abide1"), "harvest")
    assert found["jobs"].default is None
    helps = HarvestOptions.helps(load("abide1"), "harvest")
    assert "s3 4, nitrc 1, xnat 1" in helps["jobs"]
    assert "nitrc, xnat (some files) do not verify" in helps["insecure"]
    # a dataset default replaces those of the sources
    helps = HarvestOptions.helps(load("hbn"), "harvest")
    assert helps["jobs"] == "Number of parallel downloads"
    assert helps["insecure"] == "Do not verify TLS certificates"
