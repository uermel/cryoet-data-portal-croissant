from datetime import datetime, time

import cryoet_data_portal as cdp
import mlcroissant as mlc
from mlcroissant import FileObject

from cryoet_data_portal_croissant.generators.dump_tables import dump_portal


def author_to_person(
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


def dataset_metadata(
    dataset_id: int,
    distribution: list[FileObject | mlc.FileSet],
    recordsets: list[mlc.RecordSet],
) -> mlc.Metadata:
    """
    Create metadata for a cryoet_data_portal dataset.

    Args:
        dataset_id: The cryoet_data_portal dataset ID.
        distribution: The distribution of the dataset (file objects and filesets).

    Returns:
        mlc.Metadata: The generated metadata.
    """
    # Get the dataset from cryoet_data_portal
    client = cdp.Client()
    dataset = cdp.Dataset.get_by_id(client, dataset_id)

    # Create the distribution
    metadata = mlc.Metadata(
        name=dataset.title,
        description=dataset.description,
        creators=author_to_person(dataset),
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


def generate_mlcroissant_dataset(dataset_id: int, out_dir: str, data_url: str) -> mlc.Metadata:
    """
    Generate a mlcroissant dataset from a cryoet_data_portal dataset.

    Args:
        dataset_id: The cryoet_data_portal dataset ID.
        out_dir: The output directory to save the JSON files.
        data_url: The URL to the data.

    Returns:
        mlc.Metadata: The generated metadata.
    """

    # Dump the portal metadata to croissant
    distribution, recordsets = dump_portal(dataset_id, out_dir, data_url)

    # Create useful joins
    # joins = create_joins()

    # Add the joins to the recordsets
    # recordsets.extend(joins)

    # Create the dataset metadata

    metadata = dataset_metadata(dataset_id, distribution, recordsets)

    # print(json.dumps(metadata.to_json(), indent=4))

    return metadata
