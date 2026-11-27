"""Keys of the Bullitt harvest (the roast has its own, `roaster.KEYS`)."""
from brainspresso.utils.keys import get_leaves

allkeys: dict = {
    "all": {
        "meta": {},
        "raw": {
            "anat": {
                "T1w": {"flash", "mprage"},
                "T2w": {},
                "angio": {},
            },
            "dwi": {},          # downloaded, not converted
        },
        "derivatives": {
            "skullstripped": {},
            "vessels": {},
        },
        "phantoms": {},
    }
}

allleaves: set[str] = get_leaves(allkeys)
