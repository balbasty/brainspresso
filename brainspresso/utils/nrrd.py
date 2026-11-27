"""
Conversion from NRRD (used by Slicer, DTIPrep, ...) to NIfTI.

* The affine is built from `space directions` and `space origin`, and
  converted from the NRRD space (e.g. left-posterior-superior) to RAS.
* Diffusion-weighted NRRDs (`modality:=DWMRI`) are converted to a 4D
  NIfTI, with b-values and b-vectors in the BIDS (FSL) convention:
  - b-value = `DWMRI_b-value` × |g|² (Slicer convention: gradient
    norms encode the b-value scaling);
  - b-vector = unit gradient, rotated from the measurement frame to
    world space, then to voxel axes; its x component is flipped when the
    NIfTI affine has a positive determinant (as FSL and dcm2niix do).

Requires `pynrrd` (`pip install brainspresso[nrrd]`).
"""
from __future__ import annotations

from pathlib import Path

import nibabel as nib
import numpy as np

# NRRD space -> sign flips to RAS
SPACES = {
    "right-anterior-superior": (1, 1, 1),
    "RAS": (1, 1, 1),
    "left-anterior-superior": (-1, 1, 1),
    "LAS": (-1, 1, 1),
    "left-posterior-superior": (-1, -1, 1),
    "LPS": (-1, -1, 1),
    "scanner-xyz": (1, 1, 1),
}
SPATIAL_KINDS = {"domain", "space"}


def _spatial_axes(header: dict) -> list[int]:
    kinds = header.get("kinds")
    ndim = int(header["dimension"])
    if kinds:
        return [i for i, k in enumerate(kinds) if k in SPATIAL_KINDS]
    return list(range(min(ndim, 3)))


def nrrd_affine(header: dict) -> tuple[np.ndarray, np.ndarray]:
    """
    Return `(affine_ras, directions)`.

    `directions` (3x3) has the world-space step of each spatial axis as
    columns, in the NRRD space (before conversion to RAS).
    """
    space = header.get("space", "right-anterior-superior")
    if space not in SPACES:
        raise ValueError(f"Unsupported NRRD space: {space}")
    axes = _spatial_axes(header)
    if len(axes) != 3:
        raise ValueError(f"Expected 3 spatial axes, got {len(axes)}")
    dirs = np.asarray(header["space directions"], dtype=float)
    directions = np.stack([dirs[i] for i in axes], axis=1)    # columns
    origin = np.asarray(
        header.get("space origin", np.zeros(3)), dtype=float
    )
    flip = np.diag(SPACES[space]).astype(float)
    affine = np.eye(4)
    affine[:3, :3] = flip @ directions
    affine[:3, 3] = flip @ origin
    return affine, directions


def dwi_gradients(header: dict, directions: np.ndarray, affine: np.ndarray):
    """
    Return `(bvals, bvecs)` (shapes (N,) and (3, N)) in the FSL convention.
    """
    bmax = float(header["DWMRI_b-value"])
    keys = sorted(k for k in header if k.startswith("DWMRI_gradient_"))
    grads = np.asarray(
        [[float(x) for x in str(header[k]).split()] for k in keys]
    )
    if any(k.startswith("DWMRI_NEX_") for k in header):
        raise NotImplementedError("DWMRI_NEX (repeated gradients)")

    # Measurement frame: each vector is a column of the matrix that maps
    # measurement coordinates to world (NRRD space) coordinates.
    frame = header.get("measurement frame")
    frame = np.eye(3) if frame is None else np.asarray(frame, float).T

    norms = np.linalg.norm(grads, axis=1)
    bvals = np.where(norms > 1e-6, bmax * norms ** 2, 0.0)
    unit = np.zeros_like(grads)
    nz = norms > 1e-6
    unit[nz] = grads[nz] / norms[nz, None]

    world = unit @ frame.T                          # rows: world vectors
    axes = directions / np.linalg.norm(directions, axis=0)
    voxel = world @ np.linalg.inv(axes).T           # rows: voxel vectors
    if np.linalg.det(affine[:3, :3]) > 0:
        voxel[:, 0] *= -1
    voxel[~nz] = 0
    return np.round(bvals, 2), voxel.T


def nrrd_to_nifti(
    inp: str | Path,
    out: str | Path,
    *,
    bval: str | Path | None = None,
    bvec: str | Path | None = None,
) -> nib.Nifti1Image:
    """
    Convert a NRRD file to NIfTI.

    Parameters
    ----------
    inp : path
        Input `.nrrd` (or `.nhdr`) file
    out : path
        Output `.nii` or `.nii.gz` file
    bval, bvec : path, optional
        Output b-values/vectors (only for diffusion-weighted NRRDs).
        Required if the input is a DWI.
    """
    # Load-bearing: the UNC-Wisconsin roaster (imported by its dataset at
    # registration) imports this module without pynrrd being installed.
    import nrrd  # noqa: PLC0415  lazy: optional dependency (pynrrd)

    try:
        data, header = nrrd.read(str(inp))
    except nrrd.NRRDError as e:
        if "does not equal the product" in str(e):
            raise ValueError(
                f"{Path(inp).name} is truncated (the data is shorter than "
                f"its header declares); the source file may be corrupted"
            ) from e
        raise
    affine, directions = nrrd_affine(header)

    # Move non-spatial axes (e.g. diffusion "list") last
    axes = _spatial_axes(header)
    others = [i for i in range(data.ndim) if i not in axes]
    data = np.transpose(data, axes + others)

    img = nib.Nifti1Image(data, affine)
    img.set_qform(affine, code=1)
    img.set_sform(affine, code=1)
    zooms = np.linalg.norm(directions, axis=0)
    img.header.set_zooms(tuple(zooms) + (1.0,) * (data.ndim - 3))
    img.header.set_xyzt_units("mm")

    if str(header.get("modality", "")).upper() == "DWMRI":
        if bval is None or bvec is None:
            raise ValueError("DWI: bval and bvec outputs are required")
        bvals, bvecs = dwi_gradients(header, directions, affine)
        if len(bvals) != data.shape[-1]:
            raise ValueError(
                f"{len(bvals)} gradients for {data.shape[-1]} volumes"
            )
        Path(bval).write_text(" ".join(f"{b:g}" for b in bvals) + "\n",
                              encoding="utf-8", newline="\n")
        Path(bvec).write_text("\n".join(
            " ".join(f"{x:.6f}" for x in row) for row in bvecs
        ) + "\n", encoding="utf-8", newline="\n")

    nib.save(img, str(out))
    return img
