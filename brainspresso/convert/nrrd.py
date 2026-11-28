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

A corrupt, truncated or unsupported file (unknown space, not three
spatial axes, repeated gradients, gradients that do not match the
volumes) is a `ConvertError`.

Requires `pynrrd` (`pip install brainspresso[nrrd]`).
"""
from __future__ import annotations

from pathlib import Path

import nibabel as nib
import numpy as np

from brainspresso.convert.base import ConvertError, check_size, input_errors

__all__ = ["dwi_gradients", "nrrd_affine", "nrrd_to_nifti", "sources"]

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


def sources(path: str | Path) -> list[Path]:
    """
    List the files of a NRRD (the sources of its conversion).

    Parameters
    ----------
    path : path
        `.nrrd` or `.nhdr` file

    Returns
    -------
    list[Path]
        `path`, then the data file that its `data file` field names
        (detached header, `.nhdr`), if it is one file that exists
        (never raises; read without pynrrd)
    """
    path = Path(path)
    try:
        with open(path, "rb") as f:
            for line in f:
                text = line.decode("latin1").strip()
                if not text:
                    break               # (end of the header)
                key, sep, value = text.partition(":")
                if sep and key.strip().lower() in ("data file", "datafile"):
                    value = value.lstrip("=").strip()
                    if value.split()[0] == "LIST" or "%" in value:
                        break
                    data = path.parent / value
                    return [path, data] if data.is_file() else [path]
    except (OSError, IndexError):
        pass
    return [path]


def _spatial_axes(header: dict) -> list[int]:
    """List the spatial axes of a NRRD (from its `kinds`)."""
    kinds = header.get("kinds")
    ndim = int(header["dimension"])
    if kinds:
        return [i for i, k in enumerate(kinds) if k in SPATIAL_KINDS]
    return list(range(min(ndim, 3)))


def nrrd_affine(header: dict) -> tuple[np.ndarray, np.ndarray]:
    """
    Build the RAS affine of a NRRD header.

    Parameters
    ----------
    header : dict
        NRRD header (`nrrd.read`)

    Returns
    -------
    affine : (4, 4) array
        voxel -> RAS
    directions : (3, 3) array
        The world-space step of each spatial axis as columns, in the
        NRRD space (before conversion to RAS)

    Raises
    ------
    ValueError
        An unsupported space, or not three spatial axes
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
    Compute the b-values and b-vectors of a DWI NRRD (FSL convention).

    Parameters
    ----------
    header : dict
        NRRD header (`DWMRI_b-value`, `DWMRI_gradient_<n>`)
    directions : (3, 3) array
        Steps of the spatial axes (`nrrd_affine`)
    affine : (4, 4) array
        voxel -> RAS (`nrrd_affine`)

    Returns
    -------
    bvals : (N,) array
    bvecs : (3, N) array
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

    Returns
    -------
    nib.Nifti1Image
        The image written

    Raises
    ------
    ConvertError
        If the input is corrupt, truncated or not supported (nothing is
        written); an error writing the outputs passes through
    ValueError
        A DWI without `bval` and `bvec` outputs
    """
    # Load-bearing: the UNC-Wisconsin roaster (imported by its dataset at
    # registration) imports this module without pynrrd being installed.
    import nrrd  # noqa: PLC0415  lazy: optional dependency (pynrrd)

    name = Path(inp).name
    try:
        with input_errors(name):
            # (Python integers: an absurd `sizes` would overflow pynrrd's
            # `np.prod`, at least one byte per element)
            sizes = nrrd.read_header(str(inp)).get("sizes")
            if sizes is None:
                raise ConvertError(f"{name}: no sizes in the NRRD header")
            check_size(name, sizes, 1, field="sizes")
            data, header = nrrd.read(str(inp))
    except nrrd.NRRDError as e:
        if "does not equal the product" in str(e):
            raise ConvertError(
                f"{name}: truncated (the data is shorter than its "
                f"header declares); the source file may be corrupted"
            ) from e
        raise ConvertError(f"{name}: not a valid NRRD file ({e})") from e
    dwi = str(header.get("modality", "")).upper() == "DWMRI"
    if dwi and (bval is None or bvec is None):
        raise ValueError("DWI: bval and bvec outputs are required")
    with input_errors(name, "unsupported NRRD header"):
        affine, directions = nrrd_affine(header)
        # Move non-spatial axes (e.g. diffusion "list") last
        axes = _spatial_axes(header)
        others = [i for i in range(data.ndim) if i not in axes]
        data = np.transpose(data, axes + others)
    if dwi:
        with input_errors(name, "invalid DWI gradients"):
            # (`LinAlgError` is a `ValueError`: caught before
            # `input_errors` sees it)
            try:
                bvals, bvecs = dwi_gradients(header, directions, affine)
            except (NotImplementedError, np.linalg.LinAlgError) as e:
                raise ConvertError(f"{name}: unsupported DWI gradients "
                                   f"({type(e).__name__}: {e})") from e
        if len(bvals) != data.shape[-1]:
            raise ConvertError(
                f"{name}: {len(bvals)} gradients for {data.shape[-1]} "
                f"volumes"
            )

    img = nib.Nifti1Image(data, affine)
    img.set_qform(affine, code=1)
    img.set_sform(affine, code=1)
    zooms = np.linalg.norm(directions, axis=0)
    img.header.set_zooms(tuple(zooms) + (1.0,) * (data.ndim - 3))
    img.header.set_xyzt_units("mm")

    if dwi:
        Path(bval).write_text(" ".join(f"{b:g}" for b in bvals) + "\n",
                              encoding="utf-8", newline="\n")
        Path(bvec).write_text("\n".join(
            " ".join(f"{x:.6f}" for x in row) for row in bvecs
        ) + "\n", encoding="utf-8", newline="\n")

    nib.save(img, str(out))
    return img
