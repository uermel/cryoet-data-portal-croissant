import mlcroissant as mlc
import cryoet_data_portal as cdp
from typing import List, Union, Dict, Any


def tomo_to_fileobject(
    tomo: cdp.Tomogram,
) -> List[mlc.FileObject]:
    """
    Convert a cryoet_data_portal tomogram to a mlcroissant FileObject.

    Args:
        tomo: The tomogram to convert.

    Returns:
        mlc.FileObject: The converted FileObjects (one each for Zarr and MRC).
    """
    zarr_obj = mlc.FileObject(
        id = tomo.s3_omezarr_dir,
        name = tomo.s3_omezarr_dir,
        content_url = tomo.https_omezarr_dir,
        content_size=str(tomo.file_size_omezarr),
        encoding_formats=["image/OME-Zarr"],
        same_as=[tomo.s3_mrc_file],
    )

    mrc_obj = mlc.FileObject(
        id = tomo.s3_mrc_file,
        name = tomo.s3_mrc_file,
        content_url = tomo.https_mrc_file,
        content_size=str(tomo.file_size_mrc),
        encoding_formats=["image/MRC"],
        same_as=[tomo.s3_omezarr_dir],
    )

    return [zarr_obj, mrc_obj]

def tiltseries_to_fileobject(
    tiltseries: cdp.TiltSeries,
) -> List[mlc.FileObject]:
    """
    Convert a cryoet_data_portal tiltseries to a mlcroissant FileObject.

    Args:
        tiltseries: The tiltseries to convert.

    Returns:
        mlc.FileObject: The converted FileObjects (one each for Zarr and MRC).
    """
    zarr_obj = mlc.FileObject(
        id = tiltseries.s3_omezarr_dir,
        name = tiltseries.s3_omezarr_dir,
        content_url = tiltseries.https_omezarr_dir,
        content_size=str(tiltseries.file_size_omezarr),
        encoding_formats=["image/OME-Zarr"],
        same_as=[tiltseries.s3_mrc_file],
    )

    mrc_obj = mlc.FileObject(
        id = tiltseries.s3_mrc_file,
        name = tiltseries.s3_mrc_file,
        content_url = tiltseries.https_mrc_file,
        content_size=str(tiltseries.file_size_mrc),
        encoding_formats=["image/MRC"],
        same_as=[tiltseries.s3_omezarr_dir],
    )

    return [zarr_obj, mrc_obj]


def segmentation_to_fileobject(
    segmentation: cdp.AnnotationFile,
) -> List[mlc.FileObject]:
    """
    Convert a cryoet_data_portal segmentation to a mlcroissant FileObject.

    Args:
        segmentation: The segmentation to convert.

    Returns:
        mlc.FileObject: The converted FileObjects (one each for Zarr and MRC).
    """



    zarr_obj = mlc.FileObject(
        id = segmentation.s3_omezarr_dir,
        name = segmentation.s3_omezarr_dir,
        content_url = segmentation.https_omezarr_dir,
        content_size=str(segmentation.file_size_omezarr),
        encoding_formats=["image/OME-Zarr"],
        same_as=[segmentation.s3_mrc_file],
    )

    mrc_obj = mlc.FileObject(
        id = segmentation.s3_mrc_file,
        name = segmentation.s3_mrc_file,
        content_url = segmentation.https_mrc_file,
        content_size=str(segmentation.file_size_mrc),
        encoding_formats=["image/MRC"],
        same_as=[segmentation.s3_omezarr_dir],
    )

    return [zarr_obj, mrc_obj]