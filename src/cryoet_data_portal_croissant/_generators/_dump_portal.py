import hashlib
import json
from typing import Type

import cryoet_data_portal as cdp
import mlcroissant as mlc
from griffe import Docstring

_PORTAL_TYPES = (
    Type[cdp.Annotation]
    | Type[cdp.AnnotationShape]
    | Type[cdp.AnnotationFile]
    | Type[cdp.Tomogram]
    | Type[cdp.TiltSeries]
    | Type[cdp.Dataset]
    | Type[cdp.Run]
    | Type[cdp.Alignment]
)

_TYPE_MAP = {
    "str": mlc.DataType.TEXT,
    "int": mlc.DataType.INTEGER,
    "float": mlc.DataType.FLOAT,
    "bool": mlc.DataType.BOOL,
    "date": mlc.DataType.DATE,
    "datetime": mlc.DataType.DATE,
    "url": mlc.DataType.URL,
}


def _get_descriptions(cls: Type[cdp.Dataset]) -> tuple[str, dict[str, str]]:
    """
    Get the description and attributes of a class from its google docstring.

    Args:
        cls: The class to extract the docstring from.
    Returns:
        tuple: A tuple containing the description and a dictionary of attributes and their descriptions.
    """
    doc = Docstring(cls.__doc__)
    attrs = doc.parse("google")
    return attrs[0].value, {a.name: a.description for a in attrs[1].value}


def _portal_to_recordset(clz: _PORTAL_TYPES) -> mlc.RecordSet:
    """Automatically create a Pydantic model from a CryoET Data Portal annotation class."""
    attrs = clz.__annotations__
    docs = _get_descriptions(clz)
    fields = []
    clz_name = clz._gql_type.lower()
    filename = f"{clz._gql_root_field}.json"

    for name, typ in attrs.items():
        if name == "neuroglancer_config":
            continue

        if typ in ["int", "float", "str", "bool"] and name[0] != "_":
            data_type = _TYPE_MAP[typ]

            if typ == "str" and ("http" in name or "s3" in name):
                data_type = mlc.DataType.URL

            if "id" in name and name != "id":
                fld = mlc.Field(
                    id=f"{clz_name}_{name}",
                    name=name,
                    data_types=[data_type],
                    description=docs[1].get(name, None),
                    source=mlc.Source(
                        file_object=filename,
                        extract=mlc.Extract(json_path=f"$[*].{name}"),
                    ),
                    references=mlc.Source(id=f"{name}"),
                )
            else:
                fld = mlc.Field(
                    id=f"{clz_name}_{name}",
                    name=name,
                    data_types=[data_type],
                    description=docs[1].get(name, None),
                    source=mlc.Source(
                        file_object=filename,
                        extract=mlc.Extract(json_path=f"$[*].{name}"),
                    ),
                )

            fields.append(fld)

    rs = mlc.RecordSet(
        id=f"{clz_name}",
        name=f"{clz_name}",
        description=docs[0],
        fields=fields,
        key=[f"{clz_name}_id"],
    )
    return rs


def _dump_portal(
    dataset_id: int,
    out_dir: str,
    data_url: str,
) -> tuple[list[mlc.FileObject], list[mlc.RecordSet]]:
    """
    Dump a CryoET Data Portal class to a list of FileObjects.

    Args:
        dataset_id: The dataset ID to filter the query.
        out_dir: The output directory to save the JSON files.

    Returns:
        list[mlc.FileObject]: A list of FileObjects.
    """

    out_dir.strip("/")
    to_dump = [
        {
            "class": cdp.Annotation,
            "query": [cdp.Annotation.run.dataset_id == dataset_id],
        },
        {
            "class": cdp.AnnotationShape,
            "query": [cdp.AnnotationShape.annotation.run.dataset_id == dataset_id],
        },
        {
            "class": cdp.AnnotationFile,
            "query": [cdp.AnnotationFile.tomogram_voxel_spacing.run.dataset_id == dataset_id],
        },
        {
            "class": cdp.Tomogram,
            "query": [cdp.Tomogram.run.dataset_id == dataset_id],
        },
        {
            "class": cdp.TiltSeries,
            "query": [cdp.TiltSeries.run.dataset_id == dataset_id],
        },
        {
            "class": cdp.Dataset,
            "query": [cdp.Dataset.id == dataset_id],
        },
        {
            "class": cdp.Run,
            "query": [cdp.Run.dataset_id == dataset_id],
        },
        {
            "class": cdp.Alignment,
            "query": [cdp.Alignment.run.dataset_id == dataset_id],
        },
    ]

    objects = []
    recordsets = []

    for dump in to_dump:
        clz = dump["class"]
        query = dump["query"]
        client = cdp.Client()
        items = clz.find(client, query)  # noqa
        jitems = [item.to_dict() for item in items]
        name = f"{clz._gql_root_field}.json"
        filename = f"{out_dir}/{clz._gql_root_field}.json"
        text = json.dumps(jitems, indent=4, default=str) + "\n"

        with open(filename, "w") as f:
            f.write(text)

        with open(filename, "rb") as f:
            checksum = hashlib.file_digest(f, hashlib.sha256).hexdigest()

        obj = mlc.FileObject(
            id=name,
            name=name,
            description="description",
            content_url=f"http://0.0.0.0:8000/{clz._gql_root_field}.json",
            encoding_formats=["application/json"],
            sha256=checksum,
        )

        objects.append(obj)

        recordsets.append(_portal_to_recordset(clz))

    return objects, recordsets
