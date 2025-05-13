from concurrent.futures import ProcessPoolExecutor, as_completed

import cryoet_data_portal as cdp

from cryoet_data_portal_croissant.generators.dataset import generate_mlcroissant_dataset


def generate_mlcroissant(
    dataset_ids: list[int] | None = None,
    out_dir: str = "testdata",
    data_url: str = "http://[::]:8000",
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
    # with ProcessPoolExecutor() as executor:
    #     futures = [executor.submit(generate_mlcroissant_dataset, dsid, out_dir, data_url) for dsid in dataset_ids]
    #
    #     for fut in as_completed(futures):
    #         ds = fut.result()
    #         # print(ds.issues.report())
    #         ret.append(ds)

    for dsid in dataset_ids:
        ds = generate_mlcroissant_dataset(dsid, out_dir, data_url)
        # print(ds.issues.report())
        ret.append(ds)

    return ret


if __name__ == "__main__":
    import json
    import shutil

    import mlcroissant as mlc

    shutil.rmtree("/Users/utz.ermel/.cache/croissant", ignore_errors=True)

    metadata = generate_mlcroissant(
        [10440], out_dir="/Users/utz.ermel/Documents/repos/cryoet-data-portal-croissant/.notebook/testdata"
    )

    with open("/Users/utz.ermel/Documents/repos/cryoet-data-portal-croissant/.notebook/test.json", "w") as f:
        f.write(json.dumps(metadata[0].to_json(), indent=4, default=str) + "\n")

    dataset = mlc.Dataset.from_metadata(metadata[0])
    dataset.debug = True
    records = dataset.records(record_set="tomogram_type")

    for record in records:
        print(record)
