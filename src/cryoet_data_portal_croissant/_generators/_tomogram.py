from typing import List

import cryoet_data_portal as cdp
import mlcroissant as mlc


def _tomo_fileset_name(
    dataset: cdp.Dataset,
    filetype: str = "ome-zarr",
) -> str:
    """
    Generate a name for the tomogram fileset based on the dataset ID and type.

    Args:
        dataset: The cryoet_data_portal dataset.
        filetype: The type of fileset (default is "ome-zarr").

    Returns:
        str: The generated fileset name.
    """
    return f"{dataset.id}-tomograms-{filetype}"


def _tomo_filesets(
    dataset: cdp.Dataset,
) -> List[mlc.FileSet]:
    """
    Create tomogram filesets from a cryoet_data_portal dataset (one each for Zarr and MRC).
    """
    zarr_set = mlc.FileSet(
        id=_tomo_fileset_name(dataset, "ome-zarr"),
        name=_tomo_fileset_name(dataset, "ome-zarr"),
        description="Tomograms in OME-Zarr format",
        encoding_formats=["image/OME-Zarr"],
        includes=[f"{dataset.https_prefix}*/Reconstructions/VoxelSpacing*/Tomograms/*/*.zarr"],
    )

    mrc_set = mlc.FileSet(
        id=_tomo_fileset_name(dataset, "mrc"),
        name=_tomo_fileset_name(dataset, "mrc"),
        description="Tomograms in MRC format",
        encoding_formats=["image/MRC"],
        includes=[f"{dataset.https_prefix}*/Reconstructions/VoxelSpacing*/Tomograms/*/*.mrc"],
    )

    return [zarr_set, mrc_set]


def _tomo_to_fileobject(
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
    zarr_name = tomo.s3_omezarr_dir.replace("s3://cryoet-data-portal-public/", "")
    mrc_name = tomo.s3_mrc_file.replace("s3://cryoet-data-portal-public/", "")

    zarr_obj = mlc.FileObject(
        id=zarr_name,
        name=zarr_name,
        description=f"Tomogram {tomo.id} in OME-Zarr format",
        content_url=tomo.https_omezarr_dir,
        content_size=str(tomo.file_size_omezarr),
        encoding_formats=["image/OME-Zarr"],
        same_as=[mrc_name],
        contained_in=[_tomo_fileset_name(dataset, "ome-zarr")],
    )

    mrc_obj = mlc.FileObject(
        id=mrc_name,
        name=mrc_name,
        description=f"Tomogram {tomo.id} in MRC format",
        content_url=tomo.https_mrc_file,
        content_size=str(tomo.file_size_mrc),
        encoding_formats=["image/MRC"],
        same_as=[zarr_name],
        contained_in=[_tomo_fileset_name(dataset, "mrc")],
    )

    return [zarr_obj, mrc_obj]


def _tomo_recordset(
    dataset: cdp.Dataset,
    runs: List[cdp.Run],
) -> List[mlc.RecordSet]:
    """
    Create a RecordSet for tomograms.

    Returns:
        mlc.RecordSet: The created RecordSet.
    """
    run_set = mlc.RecordSet(
        id="runs",
        name="Runs",
        description=f"Runs from cryoET data portal dataset {dataset.id}.",
        key=["runs/name"],
        fields=[
            mlc.Field(
                id="runs/id",
                data_types=[mlc.DataType.INTEGER],
                description="ID of the run on the cryoET data portal",
            ),
            mlc.Field(
                id="runs/name",
                data_types=[mlc.DataType.TEXT],
                description="Name of the run on the cryoET data portal",
            ),
        ],
        data=[{"runs/id": run.id, "runs/name": f"{run.dataset_id}/{run.name}"} for run in runs],
    )

    tomo_set = mlc.RecordSet(
        id="tomograms",
        name="Tomograms",
        description=f"Tomograms from cryoET data portal dataset {dataset.id}.",
        # key=["tomograms/id"],
        fields=[
            mlc.Field(
                id="tomograms/run_name",
                data_types=[mlc.DataType.INTEGER],
                description="ID of the run this tomogram belongs to.",
                references=mlc.Source(id="runs/id"),
                source=mlc.Source(
                    file_set=_tomo_fileset_name(dataset, "mrc"),
                    extract=mlc.Extract(
                        file_property=mlc.FileProperty.fullpath,
                    ),
                    transforms=[
                        mlc.Transform(
                            regex="(.*)/Reconstructions/VoxelSpacing.*/Tomograms/.*.mrc",
                        ),
                    ],
                ),
            ),
            # mlc.Field(
            #     id="tomograms/zarr_file",
            #     data_types=[mlc.DataType.INTEGER],
            #     description="ID of the tomogram on the cryoET data portal",
            #     source=mlc.Source(file_set=_tomo_fileset_name(dataset, "ome-zarr")),
            # ),
        ],
    )

    return [run_set, tomo_set]
