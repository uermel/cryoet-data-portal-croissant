from concurrent.futures import ProcessPoolExecutor, as_completed

import cryoet_data_portal as cdp

from cryoet_data_portal_croissant._generators._dataset import _generate_mlcroissant_dataset


def generate_mlcroissant(
    dataset_ids: list[int] | None = None,
):
    """
    Generate a mlcroissant structure graph for the given dataset IDs.

    Args:
        dataset_ids: A list of dataset IDs to generate the structure graph for. If None, all datasets are used.

    Returns:
        mlcroissant.StructureGraph: The generated mlcroissant structure graph.
    """

    # Get the datasets from cryoet_data_portal
    if dataset_ids is None:
        client = cdp.Client()
        datasets = cdp.Dataset.find(client, [])
        dataset_ids = [dataset.id for dataset in datasets]

    # Create a ProcessPoolExecutor to parallelize the processing of datasets
    ret = []
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(_generate_mlcroissant_dataset, dsid) for dsid in dataset_ids]

        for fut in as_completed(futures):
            ds = fut.result()
            print(ds.issues.report())
            ret.append(ds)

    return ret
