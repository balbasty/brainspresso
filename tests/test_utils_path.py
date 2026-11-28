"""`utils.path`: platform-independent order of paths."""
from pathlib import PurePosixPath, PureWindowsPath

from brainspresso.utils.path import posix_key, sorted_paths


def test_sorted_paths_case_sensitive_on_windows():
    """`sorted` of Windows paths ignores case; `sorted_paths` does not"""
    names = ["anat1.tar.gz", "OAS30001_Freesurfer53_d0129.tar.gz"]
    windows = [PureWindowsPath("C:/x/OAS30001_MR_d0129", n) for n in names]
    posix = [PurePosixPath("/x/OAS30001_MR_d0129", n) for n in names]
    assert [p.name for p in sorted(windows)] == names
    expected = [p.name for p in sorted(posix)]
    assert expected == names[::-1]
    assert [p.name for p in sorted_paths(windows)] == expected
    assert [p.name for p in sorted_paths(posix)] == expected


def test_posix_key_compares_parts():
    """Parts are compared, as `sorted` does on POSIX (`a/b` < `a-c`)"""
    paths = ["a-c", "a/b"]
    assert sorted(paths, key=posix_key) == ["a/b", "a-c"]
    assert sorted_paths(map(PurePosixPath, paths)) == \
        sorted(map(PurePosixPath, paths))
