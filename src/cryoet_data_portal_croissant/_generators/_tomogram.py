from typing import List

import cryoet_data_portal as cdp
import mlcroissant as mlc


def tomo_fileset_name(
        dataset: cdp.Dataset,
        type: str = "ome-zarr",
) -> str:
    """
    Generate a name for the tomogram fileset based on the dataset ID and type.

    Args:
        dataset: The cryoet_data_portal dataset.
        type: The type of fileset (default is "ome-zarr").

    Returns:
        str: The generated fileset name.
    """
    return f"{dataset.id}-tomograms-{type}"


def tomo_filesets(
        dataset: cdp.Dataset,
) -> List[mlc.FileSet]:
    """
    Create tomogram filesets from a cryoet_data_portal dataset (one each for Zarr and MRC).
    """
    zarr_set = mlc.FileSet(
        id = tomo_fileset_name(dataset, "ome-zarr"),
        name= tomo_fileset_name(dataset, "ome-zarr"),
        description = "Tomograms in OME-Zarr format",
        encoding_formats=["image/OME-Zarr"],
        includes = [f"{dataset.https_prefix}*/Reconstructions/VoxelSpacing*/Tomograms/*/*.zarr"],
    )

    mrc_set = mlc.FileSet(
        id = tomo_fileset_name(dataset, "mrc"),
        name= tomo_fileset_name(dataset, "mrc"),
        description = "Tomograms in MRC format",
        encoding_formats=["image/MRC"],
        includes = [f"{dataset.https_prefix}*/Reconstructions/VoxelSpacing*/Tomograms/*/*.mrc"],
    )

    return [zarr_set, mrc_set]

def tomo_to_fileobject(
        dataset: cdp.Dataset,
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
        contained_in = [tomo_fileset_name(dataset, "ome-zarr")],
    )

    mrc_obj = mlc.FileObject(
        id = tomo.s3_mrc_file,
        name = tomo.s3_mrc_file,
        content_url = tomo.https_mrc_file,
        content_size=str(tomo.file_size_mrc),
        encoding_formats=["image/MRC"],
        same_as=[tomo.s3_omezarr_dir],
        contained_in = [tomo_fileset_name(dataset, "mrc")],
    )

    return [zarr_obj, mrc_obj]
