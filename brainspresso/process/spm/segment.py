from pathlib import Path
from typing import Literal, Iterator

from pydantic import Model

from brainspresso.actions.action import Action


class SegmentConfig(Model):

    # --- Outputs ------------------------------------------------------
    path_corrected: str = "{app}/sub-{sub:d}/anat/{base}_space-native_desc-corrected_{suffix}.nii.gz"
    save_corrected: bool | list[int] = False
    """
    Option to save an intensity nonuniformity (INU) corrected version of
    your images.

    MR images are usually corrupted by a smooth, spatially varying artifact
    that modulates the intensity of the image. These artifacts, although
    not usually a problem for visual inspection, can impede automated
    processing of the images.  The INU corrected version should have more
    uniform intensities within the different types of tissues.
    """

    path_biasfield: str = "{app}/sub-{sub:d}/anat/{base}_space-native_inu.nii.gz"
    save_biasfield: bool | list[int] = False
    """
    Option to save the estimated intensity nonuniformity (INU) field.
    """

    path_native: str = "{app}/sub-{sub:d}/anat/{base}_space-native_label-{class:d}_probseg.nii.gz"
    save_native: bool | list[int] = [1, 2, 3, 4, 5]
    """
    The native space option allows you to produce a tissue class image
    (``c*.nii``) that is in alignment with the original.
    """

    path_imported: str = "{app}/sub-{sub:d}/anat/{base}_space-imported_label-{class:d}_probseg.nii.gz"
    save_imported: bool | list[int] = False
    """
    It can also be used for "importing" into a form that can be used with
    the Shoot or Dartel toolboxes (``rc*.nii``).'};
    """

    save_warped: str = "{app}/sub-{sub:d}/anat/{base}_space-warped_label-{class:d}_desc-unmodulated_probseg.nii.gz"
    save_warped: bool | list[int] = False
    """
    You can produce spatially normalised versions of the tissue class - both
    with (``mwc*.nii``) and without (``wc*.nii``) modulation (see below).
    These can be used for voxel-based morphometry. All you need to do is
    smooth them and do the stats.
    """

    path_modulated: str = "{app}/sub-{sub:d}/anat/{base}_space-warped_label-{class:d}_desc-modulated_probseg.nii.gz"
    save_modulated: bool | list[int] = False
    """
    "Modulation" is to compensate for the effect of spatial normalisation.
    When warping a series of images to match a template, it is inevitable that
    volumetric differences will be introduced into the warped images.
    For example, if one subject''s temporal lobe has half the volume of that
    of the template, then its volume will be doubled during spatial
    normalisation. This will also result in a doubling of the voxels labelled
    grey matter.  In order to remove this confound, the spatially normalised
    grey matter (or other tissue class) is adjusted by multiplying by its
    relative volume before and after warping.  If warping results in a region
    doubling its volume, then the correction will halve the intensity of the
    tissue label. This whole procedure has the effect of preserving the total
    amount of grey matter signal in the normalised partitions.  Actually, in
    this version of SPM the warped data are not scaled by the Jacobian
    determinants when generating the "modulated" data.  Instead, the original
    voxels are projected into their new location in the warped images.
    This exactly preserves the tissue count, but has the effect of introducing
    aliasing artifacts - especially if the original data are at a lower
    resolution than the warped images.  Smoothing should reduce this artifact
    though.

    Note also that the "unmodulated" data are generated slightly differently
    in this version of SPM. In this version, the projected data are corrected
    using a kind of smoothing procedure. This is not done exactly as it should
    be done (to save computational time), but it does a reasonable job. It
    also has the effect of extrapolating the warped tissue class images beyond
    the range of the original data.  This extrapolation is not perfect, as it
    is only an estimate, but it may still be a good thing to do.
    """

    path_forward: str = "{app}/sub-{sub:d}/anat/{base}_from-native_to-warped_mode-image_xfm.nii.gz"
    save_forward: True
    path_inverse: str = "{app}/sub-{sub:d}/anat/{base}_from-warped_to-natived_mode-image_xfm.nii.gz"
    save_inverse: True
    """
    Deformation fields can be saved to disk, and used by the Deformations
    Utility. For spatially normalising images to MNI space, you will need
    the forward deformation, whereas for spatially normalising (eg) GIFTI
    surface files, you'll need the inverse. It is also possible to transform
    data in MNI space on to the individual subject, which also requires the
    inverse transform. Deformations are saved as .nii files, which contain
    three volumes to encode the x, y and z coordinates.
    """

    bbox: tuple[tuple[float, float, float], tuple[float, float, float]] = [[float('nan')]*3, [float('nan')]*3]
    """
    The bounding box (in mm) of the volume which is to be written (relative
    to the anterior commissure).
    """

    vox: float = float('nan')
    """
    The voxel size (isotropic, in mm) of the written normalised or imported
    images.
    """

    # --- Bias field ---------------------------------------------------
    biasreg: float = 1e-4
    """
    MR images are usually corrupted by a smooth, spatially varying artifact
    that modulates the intensity of the image (intensity nonuniformity - INU).
    These artifacts, although not usually a problem for visual inspection,
    can impede automated processing of the images.

    An important issue relates to the distinction between intensity variations
    that arise because of INU due to the physics of MR scanning, and those
    that arise due to different tissue properties.  The objective is to model
    the latter by different tissue classes, while modelling the former with an
    INU field. We know a priori that intensity variations due to MR physics
    tend to be spatially smooth, whereas those due to different tissue types
    tend to contain more high frequency information. A more accurate estimate
    of an INU field can be obtained by including prior knowledge about the
    distribution of the fields likely to be encountered by the correction
    algorithm. For example, if it is known that there is little or no
    intensity non-uniformity, then it would be wise to penalise large values
    for the intensity non-uniformity parameters. This regularisation can be
    placed within a Bayesian context, whereby the penalty incurred is the
    negative logarithm of a prior probability for any particular pattern
    of non-uniformity.

    Knowing what works best should be a matter of empirical exploration.
    For example, if your data has very little intensity non-uniformity
    artifact, then the INU regularisation should be increased. This
    effectively tells the algorithm that there is very little INU in
    your data, so it does not try to model it.

    * no regularisation (0)
    * extremely light regularisation (0.000001)
    * very light regularisation (0.00001)
    * light regularisation (0.0001)
    * medium regularisation (0.001)
    * heavy regularisation (0.01)
    * very heavy regularisation (0.1)
    * extremely heavy regularisation (1)
    """

    biasfwhm: float = 60
    """
    FWHM of Gaussian smoothness of the intensity nonuniformity (INU).
    If the intensity non-uniformity is very smooth, then choose a large FWHM.
    This will prevent the algorithm from trying to model out intensity
    variation due to different tissue types. The model for intensity
    non-uniformity is one of i.i.d. Gaussian noise that has been smoothed
    by some amount, before taking the exponential. Note also that smoother
    INU fields need fewer parameters to describe them. This means that the
    algorithm is faster for smoother intensity non-uniformities.

    * 30mm cutoff
    * 40mm cutoff
    * 50mm cutoff
    * 60mm cutoff
    * 70mm cutoff
    * 80mm cutoff
    * 90mm cutoff
    * 100mm cutoff
    * 110mm cutoff
    * 120mm cutoff
    * 130mm cutoff
    * 140mm cutoff
    * 150mm cutoff
    * No correction (Inf)
    """

    # --- Classes ------------------------------------------------------
    tpm: str | list[str] | None = None
    """
    Select the tissue probability image for each class. These should be maps
    of eg grey matter, white matter or cerebro-spinal fluid probability.
    A nonlinear deformation field is estimated that best overlays the tissue
    probability maps on the individual subjects'' image.

    The model is refined further by allowing the tissue probability maps to
    be deformed according to a set of estimated parameters. This allows
    spatial normalisation and segmentation to be combined into the same model.
    """

    ngauss: list[int] = [1, 1, 2, 3, 4, 2]
    """
    The number of Gaussians used to represent the intensity distribution
    for each tissue class can be greater than one. In other words, a
    tissue probability map may be shared by several clusters. The assumption
    of a single Gaussian distribution for each class does not hold for a
    number of reasons. In particular, a voxel may not be purely of one tissue
    type, and instead contain signal from a number of different tissues
    (partial volume effects). Some partial volume voxels could fall at the
    interface between different classes, or they may fall in the middle of
    structures such as the thalamus, which may be considered as being either
    grey or white matter. Various other image segmentation approaches use
    additional clusters to model such partial volume effects. These generally
    assume that a pure tissue class has a Gaussian intensity distribution,
    whereas intensity distributions for partial volume voxels are broader,
    falling between the intensities of the pure classes. Unlike these partial
    volume segmentation approaches, the model adopted here simply assumes
    that the intensity distribution of each class may not be Gaussian, and
    assigns belonging probabilities according to these non-Gaussian
    distributions. Typical numbers of Gaussians could be two for grey matter,
    two for white matter, two for CSF, three for bone, four for other soft
    tissues and two for air (background).

    Note that if any of the Num. Gaussians is set to non-parametric, then a
    non-parametric approach will be used to model the tissue intensities.
    This may work for some images (eg CT), but not others - and it has not
    been optimised for multi-channel data. Note that it is likely to be
    especially problematic for images with poorly behaved intensity histograms
    due to aliasing effects that arise from having discrete values on the
    images.
    """

    # --- Postprocessing -----------------------------------------------
    mrf: int = 1
    """
    When tissue class images are written out, a few iterations of a simple
    Markov Random Field (MRF) cleanup procedure are run.  This parameter
    controls the strength of the MRF. Setting the value to zero will disable
    the cleanup.
    """

    cleanup: Literal[0, 1, 2] = 1
    """
    This uses a crude routine for extracting the brain from segmented images.
    It begins by taking the white matter, and eroding it a couple of times
    to get rid of any odd voxels.  The algorithm continues on to do
    conditional dilations for several iterations, where the condition is
    based upon gray or white matter being present.This identified region is
    then used to clean up the grey and white matter partitions.  Note that
    the fluid class will also be cleaned, such that aqueous and vitreous
    humour in the eyeballs, as well as other assorted fluid regions (except
    CSF) will be removed.

    If you find pieces of brain being chopped out in your data, then you may
    wish to disable or tone down the cleanup procedure. Note that the
    procedure uses a number of assumptions about what each tissue class
    refers to.  If a different set of tissue priors are used, then this
    routine should be disabled.

    Note that the TPM.nii file usually used for segmentation includes
    eyeballs in the CSF class, meaning that the core algorithm includes
    these as CSF.  Those wishing to not have eyeballs in their CSF class
    should enable the Clean Up operation.  Sometimes, this may need to be
    set to do a thorough clean in order to strip them off properly.

    * 0: Dont do cleanup
    * 1: Light clean
    * 2: Thorough clean
    """

    # --- Regularisation -----------------------------------------------
    reg: list[float] = [0, 0, 0.1, 0.01, 0.04]
    """
    Registration involves simultaneously minimising two terms.  One of these
    is a measure of similarity between the images (mean-squared difference in
    the current situation), whereas the other is a measure of the roughness
    of the deformations.  This measure of roughness involves the sum of the
    following terms:

    1.  Absolute displacements need to be penalised by a tiny amount.
        The first element encodes the amount of penalty on these.
        Ideally, absolute displacements should not be penalised, but it is
        necessary for technical reasons.
    2.  The *membrane energy* of the deformation is penalised (2nd element),
        usually by a relatively small amount. This penalises the sum of
            squares of the derivatives of the velocity field (ie the sum of
                squares of the elements of the Jacobian tensors).
    3.  The *bending energy* is penalised (3rd element). This penalises the
        sum of squares of the 2nd derivatives of the velocity.
    4.  *Linear elasticity* regularisation is also included (4th and 5th
        elements).  The first parameter (mu) is similar to that for linear
        elasticity, except it penalises the sum of squares of the Jacobian
        tensors after they have been made symmetric (by averaging with the
        transpose).  This term essentially penalises length changes, without
        penalising rotations.
    5.  The final term also relates to *linear elasticity*, and is the weight
        that denotes how much to penalise changes to the divergence of the
        velocities (lambda).  This divergence is a measure of the rate of
        volumetric expansion or contraction.

    The amount of regularisation determines the tradeoff between the terms.
    More regularisation gives smoother deformations, where the smoothness
    measure is determined by the bending energy of the deformations.
    """

    affreg: Literal[False, '', 'mni', 'eastern', 'subj', 'none', None] = 'mni'
    """
    The procedure is a local optimisation, so it needs reasonable initial
    starting estimates. Images should be placed in approximate alignment
    using the Display function of SPM before beginning. A Mutual Information
    affine registration with the tissue probability maps (D'Agostino et al,
    2004) is used to achieve approximate alignment. Note that this step does
    not include any model for intensity non-uniformity. This means that if
    the procedure is to be initialised with the affine registration, then
    the data should not be too corrupted with this artifact.If there is a
    lot of intensity non-uniformity, then manually position your image in
    order to achieve closer starting estimates, and turn off the affine
    registration.

    Affine registration into a standard space can be made more robust by
    regularisation (penalising excessive stretching or shrinking).  The
    best solutions can be obtained by knowing the approximate amount of
    stretching that is needed (e.g. ICBM templates are slightly bigger than
    typical brains, so greater zooms are likely to be needed). For example,
    if registering to an image in ICBM/MNI space, then choose this option.
    If registering to a template that is close in size, then select the
    appropriate option for this.

    * False or empty:   No affine registration
    * mni:              ICBM space template - European brains
    * eastern:          ICBM space template - East Asian brains
    * subj:             Average sized template
    * None or none:     No regularisation
    """

    fwhm: float = 0.
    """
    This is used to derive a fudge factor to account for correlations between
    neighbouring voxels. Smoother data have more spatial correlations,
    rendering the assumptions of the model inaccurate. For PET or SPECT,
    set this value to about 5 mm, or more if the images have smoother noise.
    For MRI, you can usually use a value of 0 mm.
    """

    samp: float = 3.
    """
    This encodes the approximate distance between sampled points when
    estimating the model parameters. Smaller values use more of the data,
    but the procedure is slower and needs more memory. Determining the optimal
    setting involves a compromise between speed and accuracy.
    """


class Segment:

    def __init__(self, config: SegmentConfig | None, **kwargs) -> None:
        if config and kwargs:
            config = config.model_copy(update=kwargs)
        elif kwargs:
            config = SegmentConfig(**kwargs)
        self.config = config

    def __call__(self, *channels, **filters):
        """
        """


# class SegmentWrapper:

#     def __init__(
#         self,
#         *channels,
#         config: SegmentConfig,
#         app: Path | str = "spm",
#     ) -> None:
#         self.channels = list(map(Path, channels))
#         self.config = config
#         self.app = Path(app)

#     @property
#     def sub(self) -> int:
#         channel = self.channels[0]
#         base = channel.name.split("_")[:-1]
#         args = {
#             key: "-".join(value )
#             for key_value in base
#             for key, *value in key_value.split("-")
#         }
#         return int(args["sub"])

#     @property
#     def dst(self) -> Iterator[str]:
#         sub = self.sub
#         app = self.app

#         save_corrected = self.config.save_corrected
#         if save_corrected is True:
#             save_corrected = list(range(1, len(self.channels)+1))
#             for i, channel in enumerate(self.channels, start=1):
#                 if i in save_corrected:
#                     *base, suffix = channel.name.split("_")
#                     suffix = suffix.split(".")[0]
#                     base = "_".join(base)
#                     yield self.config.path_corrected.format(
#                         app=app,
#                         sub=sub,
#                         base=base,
#                         suffix=suffix,
#                     )
