from typing import List

import cryoet_data_portal as cdp
import mlcroissant as mlc


def _alignment_fileset_name(
    dataset: cdp.Dataset,
) -> str:
    """
    Generate a name for the alignment fileset based on the dataset ID and type.

    Args:
        dataset: The cryoet_data_portal dataset.
        type: The type of fileset (default is "ome-zarr").

    Returns:
        str: The generated fileset name.
    """
    return f"{dataset.id}-alignments"


def _alignment_filesets(
    dataset: cdp.Dataset,
) -> List[mlc.FileSet]:
    """
    Create alignment filesets from a cryoet_data_portal dataset.

    Args:
        dataset: The cryoet_data_portal dataset.

    Returns:
        List[mlc.FileSet]: The generated filesets.
    """
    return [
        mlc.FileSet(
            id=_alignment_fileset_name(dataset),
            name=_alignment_fileset_name(dataset),
            description="Alignments",
            encoding_formats=["application/json"],
            includes=[f"{dataset.https_prefix}*/Alignments/*/alignment_metadata.json"],
        )
    ]


def _alignment_to_fileobject(
    dataset: cdp.Dataset,
    alignment: cdp.Alignment,
) -> List[mlc.FileObject]:
    """
    Convert a cryoet_data_portal alignment to a mlcroissant FileObject.

    Args:
        dataset: The cryoet_data_portal dataset.
        alignment: The alignment to convert.

    Returns:
        List[mlc.FileObject]: The converted FileObjects.
    """
    path = alignment.s3_alignment_metadata.replace("s3://cryoet-data-portal-public/", "")

    return [
        mlc.FileObject(
            id=path,
            name=path,
            description=f"Tomographic alignment metadata for run {alignment.run_id}.",
            content_url=alignment.https_alignment_metadata,
            encoding_formats=["application/json"],
            contained_in=[_alignment_fileset_name(dataset)],
        )
    ]
