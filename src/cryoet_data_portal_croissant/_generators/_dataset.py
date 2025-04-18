from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, time

import cryoet_data_portal as cdp
import mlcroissant as mlc
from mlcroissant import FileObject

from cryoet_data_portal_croissant._generators._tiltseries import _tiltseries_filesets, _tiltseries_to_fileobject
from cryoet_data_portal_croissant._generators._tomogram import _tomo_filesets, _tomo_to_fileobject, _tomo_recordset


def _author_to_person(
    dataset: cdp.Dataset,
) -> list[mlc.Person]:
    """
    Convert a cryoet_data_portal author to a mlcroissant Person.

    Args:
        dataset: The cryoet_data_portal dataset.

    Returns:
        mlc.Person: The converted Person.
    """
    persons = []

    for author in dataset.authors:
        person = mlc.Person(
            name=author.name,
            url=f"https://orcid.org/{author.orcid}" if author.orcid else None,
        )
        persons.append(person)

    return persons


def _dataset_metadata(
    dataset: cdp.Dataset,
    distribution: list[FileObject | mlc.FileSet],
    recordsets: list[mlc.RecordSet],
) -> mlc.Metadata:
    """
    Create metadata for a cryoet_data_portal dataset.

    Args:
        dataset: The cryoet_data_portal dataset.
        distribution: The distribution of the dataset (file objects and filesets).

    Returns:
        mlc.Metadata: The generated metadata.
    """

    metadata = mlc.Metadata(
        name=dataset.title,
        description=dataset.description,
        creators=_author_to_person(dataset),
        date_created=datetime.combine(dataset.deposition_date, time.min),
        date_modified=datetime.combine(dataset.last_modified_date, time.min),
        date_published=datetime.combine(dataset.release_date, time.min),
        license=["https://creativecommons.org/public-domain/cc0/"],
        url=f"https://cryoetdataportal.czscience.com/datasets/{dataset.id}",
        distribution=distribution,
        record_sets=recordsets,
        data_collection_type=["Physical data collection", "Direct measurement", "Experiments", "Others"],
        # TODO: Fill these in automatically
        # data_collection=dataset.sample_preparation,
        # data_collection_raw_data=dataset.data_collection_raw_data,
        # data_collection_timeframe=dataset.data_collection_timeframe,
        # data_imputation_protocol=dataset.data_imputation_protocol,
        # data_preprocessing_protocol=dataset.data_preprocessing_protocol,
        # data_manipulation_protocol=dataset.data_manipulation_protocol,
        # data_annotation_protocol=dataset.data_annotation_protocol,
        # data_annotation_platform=dataset.data_annotation_platform,
        # data_annotation_analysis=dataset.data_annotation_analysis,
        # annotations_per_item=dataset.annotations_per_item,
        # annotator_demographics=dataset.annotator_demographics,
        # machine_annotation_tools=dataset.machine_annotation_tools,
    )

    return metadata


def _generate_mlcroissant_dataset(
    dataset_id: int,
) -> mlc.Metadata:

    client = cdp.Client()
    dataset = cdp.Dataset.get_by_id(client, dataset_id)

    # Generate file objects and filesets
    tomograms = cdp.Tomogram.find(client, [cdp.Tomogram.run.dataset_id == dataset_id])
    tiltseries = cdp.TiltSeries.find(client, [cdp.TiltSeries.run.dataset_id == dataset_id])

    with ThreadPoolExecutor() as executor:
        tomogram_futures = [executor.submit(_tomo_to_fileobject, dataset, tomo) for tomo in tomograms]
        tiltseries_futures = [executor.submit(_tiltseries_to_fileobject, dataset, ts) for ts in tiltseries]

        tomogram_objects = []
        for future in as_completed(tomogram_futures):
            tomogram_objects.extend(future.result())

        tiltseries_objects = []
        for future in as_completed(tiltseries_futures):
            tiltseries_objects.extend(future.result())

    tomogram_sets = _tomo_filesets(dataset)
    tiltseries_sets = _tiltseries_filesets(dataset)

    distribution = tomogram_objects + tiltseries_objects + tomogram_sets + tiltseries_sets

    recordsets = _tomo_recordset(dataset, dataset.runs)

    # Create the dataset metadata
    metadata = _dataset_metadata(dataset, distribution, recordsets)

    return metadata
