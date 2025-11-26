# Attempt to build a general hierarchy of keys -- unlikely to fit all datasets
allkeys: dict = {
    "all": {
        "meta": {
            "demo",                     # Demographics
            "pheno",                    # Phenotypes
        },
        "raw": {
            "mri": {                    # MRI scans
                "anat": {               # Anatomical scans
                    "PDw": {},          # Proton-density weighted
                    "T1w": {            # T1-weighted (e.g., MPRAGE)
                        "mprage",
                        "mp2rage",
                        "UNIT1",        # Homogeneous T1-weighted MP2RAGE
                        "inplaneT1",    # T1w structural image matched to EPI.
                    },
                    "T2w": {            # T2-weighted
                        "TSE",          # Turbo Spin Echo
                        "inplaneT2",    # T2w structural image matched to EPI
                    },
                    "T2starw": {},      # T2*-weighted
                    "FLAIR": {},        # Fluid-inversion recovery
                    "PDT2": {},
                    "angio": {"tof"},   # Angiography (arteries)
                    "swi": {},          # Susceptibility-weighted (veins)
                    "map": {            # Parametric structural MR images
                        "Chimap",
                        "M0map",
                        "MTRmap",
                        "MTVmap",
                        "MTsat",
                        "MWFmap",
                        "PDmap",
                        "R1map",
                        "R2map",
                        "R2starmap",
                        "S0map",
                        "T1map",
                        "T1rho",
                        "T2map",
                        "T2starmap",
                        "RB1map",
                        "TB1map",
                    },
                },
                "func": {"bold", "cbv"},    # Functional scans
                "perf": {"pasl", "asl"},    # Perfusion imaging
                "fmap": {                   # B0 field mapping
                    "fmap-epi",
                    "fmap-fieldmap",
                    "fmap-magnitude",
                    "fmap-magnitude1",
                    "fmap-magnitude2",
                    "fmap-phase1",
                    "fmap-phase2",
                    "fmap-phasediff",
                },
                "dwi": {"sbref"},       # Diffusion-weighted scans
            },
            "pet": {"FDG", "PIB", "AV45", "AV1451"},
            "ct": {"CT"},
        },
        "derivatives": {
            "proc": {
                "min_proc": {},
                "fs": {
                    "fs-all"
                },              # FreeSurfer
                "ants": {},     # ANTs
                "civet": {},    # Civet
                "ccs": {},
                "cpac": {},     # Configurable Pipeline for the Analysis of Connectomes     # noqa: E501
                "dparsf": {},   # Data Processing Assistant for Resting-State fMRI          # noqa: E501
                "niak": {},     # NeuroImaging Analysis Kit @ SIMEXP
                "pup": {}       # PET Unified Pipeline
            },
            "qa": {
                "man_qa",
                "pcp_qa",
            }
        },
    }
}


aliases: dict = {
    "t2star": "t2starw",
    "fieldmap": "fmap",
}


def get_leaves(obj: dict | set | list | tuple | str) -> set[str]:
    """
    Get all keys that are leaves in the tree
    (member of a set, or dictionary keys that map to None or an empty set)
    """
    if isinstance(obj, set):
        return obj
    if isinstance(obj, str):
        return {obj}
    if isinstance(obj, (list, tuple)):
        return set().union(*map(get_leaves, obj))
    assert isinstance(obj, dict)
    return set().union(*[get_leaves(v) if v else {k} for k, v in obj.items()])


allleaves = get_leaves(allkeys)


def flatten_keys(x, superkey: str | None = None) -> set[str]:
    """
    Return the set of all keys.

    If `superkey`, only keys that are below this super-key are inluded.
    """
    if isinstance(x, dict):
        if superkey:
            if superkey in x:
                y = {superkey} | flatten_keys(x[superkey])
            else:
                y = set().union(
                    *[flatten_keys(v, superkey) for v in x.values()]
                )
        else:
            y = set().union(
                set(x.keys()), *[flatten_keys(v) for v in x.values()]
            )
    else:
        if isinstance(x, str):
            x = {x}
        assert isinstance(x, set)
        if superkey:
            y = x.intersection({superkey})
        else:
            y = set(x)
    y.discard("")
    return y


def lower_keys(key: str, keys: dict = allkeys) -> set[str]:
    """Return all keys t:hat are below `key` in the hierarchy"""
    return flatten_keys(keys, key)


def upper_keys(key: str, keys: dict = allkeys) -> set[str]:
    """Return all keys that are above `key` in the hierarchy"""
    def _impl(x):
        if isinstance(x, dict):
            if key in x.keys():
                return {key}
            else:
                keys = set()
                for k, v in x.items():
                    v = _impl(v)
                    if v:
                        keys = keys.union({k}, v)
                return keys
        else:
            if isinstance(x, str):
                x = {x}
            assert isinstance(x, set)
            if key in x:
                return {key}
            else:
                return set()
    keys = _impl(keys)
    keys.discard("")
    return keys


def compat_keys(key: str, keys: dict = allkeys) -> set[str]:
    """Return all keys that are compatible with `key`"""
    return lower_keys(key, keys).union(upper_keys(key, keys))


def lower_equal_key(x: str, y: str, keys: dict = allkeys) -> bool:
    return x in lower_keys(y, keys)


def lower_key(x: str, y: str, keys: dict = allkeys) -> bool:
    return x != y and x in lower_keys(y, keys)


def upper_equal_key(x: str, y: str, keys: dict = allkeys) -> bool:
    return x in upper_keys(y, keys)


def upper_key(x: str, y: str, keys: dict = allkeys) -> bool:
    return x != y and x in upper_keys(y, keys)
