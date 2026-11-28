"""
Volume helpers of the converters.

Affines of headerless orientations, relabelling of label volumes.
"""
from __future__ import annotations

from typing import Iterable, Mapping

import numpy as np

__all__ = ["make_affine", "relabel"]


def make_affine(shape, voxel_size=1, orient='RAS', center='(x-1)/2'):
    """
    Generate an affine matrix with (0, 0, 0) in the center of the FOV.

    Parameters
    ----------
    shape : list[int]
        Shape of the volume
    voxel_size : float | list[float]
        Voxel size
    orient : str
        Orientation of the voxel axes: a permutation of `R` or `L`, `A`
        or `P`, `S` or `I` (e.g. `"ASL"`)
    center : list[float] | {"x/2", "x//2", "(x-1)/2", "(x-1)//2"}
        Voxel coordinates of the origin

    Returns
    -------
    affine : (4, 4) array
        voxel -> world
    """
    pos_orient = {'L': 'R', 'P': 'A', 'I': 'S'}
    pos_orient = [pos_orient.get(x, x) for x in orient]
    flip_orient = {'L': -1, 'P': -1, 'I': -1}
    flip_orient = [flip_orient.get(x, 1) for x in orient]
    perm = [pos_orient.index(x) for x in 'RAS']

    lin = np.eye(3)
    lin = lin * np.asarray(flip_orient) * np.asarray(voxel_size)
    lin = lin[perm, :]

    if isinstance(center, str):
        shape = np.asarray(shape)
        if center == '(x-1)/2':
            center = (shape - 1) / 2
        elif center == '(x-1)//2':
            center = (shape - 1) // 2
        elif center == 'x/2':
            center = shape / 2
        elif center == 'x//2':
            center = shape // 2
        else:
            raise ValueError('invalid value for `center`')
    else:
        center = np.asarray(center)

    aff = np.eye(4)
    aff[:3, :3] = lin
    aff[:3, -1:] = - lin @ center[:, None]

    return aff


def relabel(data: np.ndarray,
            sources: Mapping[int, int | Iterable[int]]) -> np.ndarray:
    """
    Relabel a label volume.

    `sources` maps each NEW label to the OLD label(s) it takes, `{new:
    old}`: FSL FAST writes CSF, GM, WM as 1, 2, 3; in the SPM order (GM,
    WM, CSF as 1, 2, 3) that is `relabel(data, {1: 2, 2: 3, 3: 1})`.
    Labels that no entry takes become 0.

    Parameters
    ----------
    data : array of int
        Input label volume
    sources : dict[int, int | list[int]]
        `{new label: old label or labels}`

    Returns
    -------
    array of int
        Relabelled volume (same shape and dtype)
    """
    out = np.zeros_like(data)
    for new, old in sources.items():
        if hasattr(old, '__iter__'):
            for old1 in old:
                out[data == old1] = new
        else:
            out[data == old] = new
    return out
