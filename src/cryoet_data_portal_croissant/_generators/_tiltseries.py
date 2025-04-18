from typing import List

import cryoet_data_portal as cdp
import mlcroissant as mlc


def tiltseries_fileset_name(
        dataset: cdp.Dataset,
        type: str = "ome-zarr",
) -> str:
    """
    Generate a name for the tiltseries fileset based on the dataset ID and type.

    Args:
        dataset: The cryoet_data_portal dataset.
        type: The type of fileset (default is "ome-zarr").

    Returns:
        str: The generated fileset name.
    """
    return f"{dataset.id}-tiltseries-{type}"

def tiltseries_filesets(
        dataset: cdp.Dataset,
) -> List[mlc.FileSet]:
    """
    Create tiltseries filesets from a cryoet_data_portal dataset (one each for Zarr and MRC).
    """
    zarr_set = mlc.FileSet(
        id = tiltseries_fileset_name(dataset, "ome-zarr"),
        name= tiltseries_fileset_name(dataset, "ome-zarr"),
        description = "Tiltseries in OME-Zarr format",
        encoding_formats=["image/OME-Zarr"],
        includes = [f"{dataset.https_prefix}*/Reconstructions/VoxelSpacing*/TiltSeries/*/*.zarr"],
    )

    mrc_set = mlc.FileSet(
        id = tiltseries_fileset_name(dataset, "mrc"),
        name= tiltseries_fileset_name(dataset, "mrc"),
        description = "Tiltseries in MRC format",
        encoding_formats=["image/MRC"],
        includes = [f"{dataset.https_prefix}*/Reconstructions/VoxelSpacing*/TiltSeries/*/*.mrc"],
    )

    return [zarr_set, mrc_set]

def tiltseries_to_fileobject(
        dataset: cdp.Dataset,
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
        contained_in = [tiltseries_fileset_name(dataset, "ome-zarr")],
    )

    mrc_obj = mlc.FileObject(
        id = tiltseries.s3_mrc_file,
        name = tiltseries.s3_mrc_file,
        content_url = tiltseries.https_mrc_file,
        content_size=str(tiltseries.file_size_mrc),
        encoding_formats=["image/MRC"],
        same_as=[tiltseries.s3_omezarr_dir],
        contained_in = [tiltseries_fileset_name(dataset, "mrc")],
    )
    mlc.Field
    return [zarr_obj, mrc_obj]
