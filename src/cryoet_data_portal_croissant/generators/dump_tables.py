import os
import hashlib
import json
from typing import Type, Iterable
from collections import namedtuple

import cryoet_data_portal as cdp
import mlcroissant as mlc
from cryoet_data_portal._gql_base import (
    BooleanField,
    DateField,
    FloatField,
    IntField,
    StringField,
)
from griffe import Docstring
from mlcroissant import FileObject

# Portal tables to be dumped
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

# GQL types to mlcroissant types
_TYPE_MAP = {
    StringField: mlc.DataType.TEXT,
    IntField: mlc.DataType.INTEGER,
    FloatField: mlc.DataType.FLOAT,
    BooleanField: mlc.DataType.BOOL,
    DateField: mlc.DataType.DATE,
}


def get_query_map(dataset_id) -> dict[_PORTAL_TYPES, Iterable[cdp._gql_base.GQLExpression]]:
    """Query filters to retrieve all entities related to a dataset ID.

    Args:
        dataset_id: The dataset ID to filter the query.

    Returns:
        dict: A dictionary mapping CryoET Data Portal classes to their respective query filters.
    """
    return {
        cdp.Annotation: [cdp.Annotation.run.dataset_id == dataset_id],
        cdp.AnnotationShape: [cdp.AnnotationShape.annotation.run.dataset_id == dataset_id],
        cdp.AnnotationFile: [cdp.AnnotationFile.tomogram_voxel_spacing.run.dataset_id == dataset_id],
        cdp.Tomogram: [cdp.Tomogram.run.dataset_id == dataset_id],
        cdp.TiltSeries: [cdp.TiltSeries.run.dataset_id == dataset_id],
        cdp.Dataset: [cdp.Dataset.id == dataset_id],
        cdp.Run: [cdp.Run.dataset_id == dataset_id],
        cdp.Alignment: [cdp.Alignment.run.dataset_id == dataset_id],
    }


def get_descriptions(cls: Type[cdp.Dataset]) -> tuple[str, dict[str, str]]:
    """
    Get the description and attributes of a class from its google docstring. Used to generate mlcroissant metadata.

    Args:
        cls: The class to extract the docstring from.
    Returns:
        tuple: A tuple containing the description and a dictionary of attributes and their descriptions.
    """
    doc = Docstring(cls.__doc__)
    attrs = doc.parse("google")
    return attrs[0].value, {a.name: a.description for a in attrs[1].value}


def table_to_fields(
    clz: _PORTAL_TYPES,
) -> list[mlc.Field]:
    """
    Automatically create a list of Fields from a CryoET Data Portal client class.

    Args:
        clz: The CryoET Data Portal client class to convert to fields.

    Returns:
        list[mlc.Field]: A list of mlcroissant Fields.
    """
    # Scalar values from the table
    scalars = {n: clz.__dict__[n] for n in clz._get_scalar_fields()}
    # relationships = {n: clz.__dict__[n] for n in clz._get_relationship_fields()}

    # Get the descriptions
    docs = get_descriptions(clz)

    # Prep output
    fields = []
    clz_name = clz._gql_type.lower()
    filename = f"{clz._gql_root_field}.json"

    # Create fields
    for name, typ in scalars.items():
        if name == "neuroglancer_config":
            continue

        if isinstance(typ, (StringField, IntField, BooleanField, FloatField)) and name[0] != "_":
            data_type = _TYPE_MAP[type(typ)]

            if isinstance(typ, StringField) and ("http" in name or "s3" in name):
                data_type = mlc.DataType.URL

            if "id" in name and name != "id":
                # This is a foreign key reference to another table
                fld = mlc.Field(
                    id=f"{clz_name}/{name}",
                    name=f"{clz_name}/{name}",
                    data_types=[data_type],
                    description=docs[1].get(name, None),
                    source=mlc.Source(
                        file_object=filename,
                        extract=mlc.Extract(json_path=f"$[*].{name}"),
                    ),
                    # references=mlc.Source(id=f"{name}"),
                )
            else:
                fld = mlc.Field(
                    id=f"{clz_name}/{name}",
                    name=f"{clz_name}/{name}",
                    data_types=[data_type],
                    description=docs[1].get(name, None),
                    source=mlc.Source(
                        file_object=filename,
                        extract=mlc.Extract(json_path=f"$[*].{name}"),
                    ),
                )

            fields.append(fld)

    return fields


def table_to_recordset(clz: _PORTAL_TYPES) -> mlc.RecordSet:
    """Create a mlcroissant RecordSet from a CryoET Data Portal client class.

    Args:
        clz: The CryoET Data Portal client class to convert to a RecordSet.

    Returns:
        mlc.RecordSet: A mlcroissant RecordSet.
    """

    docs = get_descriptions(clz)
    clz_name = clz._gql_type.lower()
    fields = table_to_fields(clz)

    # Create fields
    rs = mlc.RecordSet(
        id=f"{clz_name}",
        name=f"{clz_name}",
        description=docs[0],
        fields=fields,
        key=[f"{clz_name}/id"],
    )
    return rs


def table_to_file(
    clz: _PORTAL_TYPES,
    query_filters: Iterable[cdp._gql_base.GQLExpression],
    out_dir: str,
    data_url: str,
) -> mlc.FileObject:
    """
    Dump a CryoET Data Portal class to a JSON file and create a corresponding mlcroissant FileObject.

    Args:
        clz: The CryoET Data Portal client class to convert to a FileObject.
        query_map: The query map to filter the data.
        out_dir: The output directory to save the JSON files.
        data_url: The base URL for the data file location.

    Returns:
        mlc.FileObject: A mlcroissant FileObject.
    """

    out_dir = out_dir.rstrip("/")
    data_url = data_url.rstrip("/")

    client = cdp.Client()

    jitems = []
    for item in clz.find(client, query_filters):  # noqa
        jitem = item.to_dict()

        if "neuroglancer_config" in jitem:
            jitem.pop("neuroglancer_config")

        for k, v in jitem.items():
            if v is None:
                jitem[k] = ""

        jitems.append(jitem)

    name = f"{clz._gql_root_field}.json"
    filename = f"{out_dir}/{name}"
    text = json.dumps(jitems, indent=4, default=str) + "\n"

    with open(filename, "w") as f:
        f.write(text)

    with open(filename, "rb") as f:
        checksum = hashlib.file_digest(f, hashlib.sha256).hexdigest()

    fobj = mlc.FileObject(
        id=name,
        name=name,
        description="description",
        content_url=f"{data_url}/{name}",
        encoding_formats=["application/json"],
        sha256=checksum,
    )

    return fobj


# def prefix_field_names(fields: list[mlc.Field], prefix: str) -> list[mlc.Field]:
#     """
#     Prefix the field names with the given prefix.
#
#     Args:
#         fields: The list of fields to prefix.
#         prefix: The prefix to add to the field names.
#
#     Returns:
#         list[mlc.Field]: The list of fields with prefixed names.
#     """
#     for field in fields:
#         field.id = f"{prefix}/{field.id}"
#
#     return fields


def fields_to_foreign_reference_fields(fields: list[mlc.Field], prefix: str, exclude_ids: list[str]) -> list[mlc.Field]:
    """
    Convert the fields of a RecordSet to fields referencing another RecordSet.

    Args:
        fields: The list of fields to convert.
        prefix: The prefix to add to the field names.
        exclude_ids: The list of field IDs to exclude from the conversion.

    Returns:
        list[mlc.Field]: The list of fields with prefixed names and foreign references.
    """
    ret = []

    for field in fields:
        field.source = mlc.Source(field=f"{field.id}")
        field.id = f"{prefix}/{field.id}"

        if field.id not in exclude_ids:
            ret.append(field)

    return ret


def tomo_type_recordset() -> mlc.RecordSet:
    """
    A recordset mapping each tomogram to its unique tomogram type within this dataset.

    Returns:
        mlc.RecordSet: A mlcroissant RecordSet for tomogram types.
    """
    filename = f"tomogram_types.json"

    # Create original fields
    tomotype_fields = [
        mlc.Field(
            id=f"tomogram_type/id",
            name="Tomogram Type ID",
            data_types=[mlc.DataType.INTEGER],
            description="Unique identifier for the tomogram type",
            source=mlc.Source(
                file_object=filename,
                extract=mlc.Extract(json_path=f"$[*].id"),
            ),
        ),
        mlc.Field(
            id=f"tomogram_type/name",
            name="Tomogram Type Name",
            data_types=[mlc.DataType.INTEGER],
            description="Name of the tomogram type",
            source=mlc.Source(
                file_object=filename,
                extract=mlc.Extract(json_path=f"$[*].name"),
            ),
        ),
        mlc.Field(
            id=f"tomogram_type/tomogram/id",
            name="Tomogram ID",
            data_types=[mlc.DataType.INTEGER],
            description="Unique identifier for the tomogram type",
            source=mlc.Source(
                file_object=filename,
                extract=mlc.Extract(json_path=f"$[*].tomogram_id"),
            ),
            references=mlc.Source(field="tomogram/id"),
        ),
    ]

    # Pull in the tomogram fields from the existing tomogram recordset
    tomo_fields = table_to_fields(cdp.Tomogram)
    tomo_fields = fields_to_foreign_reference_fields(
        tomo_fields, prefix="tomogram_type", exclude_ids=[f.id for f in tomotype_fields]
    )
    tomotype_fields.extend(tomo_fields)

    # Create the recordset
    rs = mlc.RecordSet(
        id="tomogram_type",
        name="Tomogram Type",
        description="A recordset mapping each tomogram to its unique tomogram type within this dataset.",
        fields=tomotype_fields,
        key=["tomogram_type/id"],
    )

    return rs


def tomo_type_fileobject(
    dataset_id: int,
    out_dir: str,
    data_url: str,
) -> FileObject:
    """
    Create the fileobject for the tomogram type recordset.

    Args:
        dataset_id: The dataset ID to filter the query.
        out_dir: The output directory to save the JSON files.
        data_url: The base URL for the data file location.

    Returns:
        list[str]: A list of unique tomogram hashes.
    """
    # Get all tomos
    client = cdp.Client()
    query_map = get_query_map(dataset_id)
    all_tomos = cdp.Tomogram.find(client, query_map[cdp.Tomogram])

    # Properties that make a tomogram unique
    TomoProps = namedtuple(
        "TomoProps",
        [
            "deposition_id",
            "voxel_spacing",
            "reconstruction_method",
            "processing",
            "processing_software",
        ],
    )

    # Get the set of unique combination of properties among all tomograms (keys of the dict)
    unique_properties = {}
    for tomo in all_tomos:
        props = TomoProps(
            deposition_id=tomo.deposition_id,
            voxel_spacing=tomo.voxel_spacing,
            reconstruction_method=tomo.reconstruction_method,
            processing=tomo.processing,
            processing_software=tomo.processing_software,
        )

        tomos = unique_properties.get(props, [])
        tomos.append(tomo)
        unique_properties[props] = tomos

    data = []
    for props, tomos in unique_properties.items():
        tomo_type_id = (
            f"{dataset_id}_{props.deposition_id}_{props.voxel_spacing}_{props.reconstruction_method}_"
            f"{props.processing}_{props.processing_software}"
        )

        for t in tomos:
            data.append(
                {
                    "id": tomo_type_id,
                    "tomogram_id": t.id,
                }
            )

    # Write the file
    name = f"tomogram_types.json"
    filename = f"{out_dir}/{name}"
    with open(filename, "w") as f:
        f.write(json.dumps(data, indent=4, default=str) + "\n")

    # Create the fileobject
    with open(filename, "rb") as f:
        checksum = hashlib.file_digest(f, hashlib.sha256).hexdigest()

    fobj = mlc.FileObject(
        id=name,
        name=name,
        description="Tomograms with unique tomogram types within this dataset.",
        content_url=f"{data_url}/{name}",
        encoding_formats=["application/json"],
        sha256=checksum,
    )

    return fobj


def dump_tomo_type(
    dataset_id: int,
    out_dir: str,
    data_url: str,
) -> tuple[mlc.FileObject, mlc.RecordSet]:
    """
    Dump the tomogram type table to JSON files and create corresponding mlcroissant FileObjects and RecordSets.

    Args:
        dataset_id: The dataset ID to filter the query.
        out_dir: The output directory to save the JSON files.
        data_url: The base URL for the data file location.

    Returns:
        tuple[mlc.FileObject, mlc.RecordSet]: A tuple containing a mlcroissant FileObject and RecordSet.
    """
    # Create the fileobject
    fobj = tomo_type_fileobject(dataset_id, out_dir, data_url)

    # Create the recordset
    rs = tomo_type_recordset()
    return fobj, rs


def dump_portal(
    dataset_id: int,
    out_dir: str,
    data_url: str,
) -> tuple[list[mlc.FileObject], list[mlc.RecordSet]]:
    """
    Dump the CryoET Data Portal tables to JSON files and create corresponding mlcroissant FileObjects and RecordSets.

    Args:
        dataset_id: The dataset ID to filter the query.
        out_dir: The output directory to save the JSON files.
        data_url: The base URL for the data file location.

    Returns:
        tuple[list[mlc.FileObject], list[mlc.RecordSet]]: A tuple containing a list of mlcroissant FileObjects and
        RecordSets.
    """
    # Sane inputs
    out_dir = out_dir.rstrip("/")
    data_url = data_url.rstrip("/")

    # Create the output directory if it doesn't exist
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    # Classes to dump
    to_dump = get_query_map(dataset_id)

    # Dump all portal tables
    recordsets = []
    fileobjects = []
    for clz, query in to_dump.items():
        recordsets.append(table_to_recordset(clz))
        fileobjects.append(table_to_file(clz, query, out_dir, data_url))

    # Add the tomogram type recordset and fileobject
    fobj, rs = dump_tomo_type(dataset_id, out_dir, data_url)
    recordsets.append(rs)
    fileobjects.append(fobj)

    return fileobjects, recordsets
