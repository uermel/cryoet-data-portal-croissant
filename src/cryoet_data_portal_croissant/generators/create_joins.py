from collections import namedtuple

import cryoet_data_portal as cdp

from cryoet_data_portal_croissant.generators.dump_portal import get_query_map

# def tomogram_segmentation_join() -> mlc.RecordSet:
#     """
#     Create a join between a tomogram recordset and its segmentation recordset.
#
#     Returns:
#         mlc.RecordSet: The generated join.
#     """
#
#     rs = mlc.RecordSet(
#         id="tomogram_segmentation",
#         name="Tomogram Segmentation",
#         description="Join between tomogram and annotation recordsets that contain segmentation information.",
#         key=["tomogram_segmentation/tomogram_id", "tomogram_segmentation/annotation_id"],
#         fields=[
#             mlc.Field(
#                 id="tomogram_segmentation/tomogram_id",
#                 name="Tomogram ID",
#                 description="ID of the tomogram",
#                 data_types=[mlc.DataType.INTEGER],
#                 references=mlc.Source(
#                     field="tomogram_id",
#                 ),
#                 source=mlc.Source(
#                     # field="tomogram_id",
#                     file_object="tomograms.json",
#                     extract=mlc.Extract(json_path="$[*].id"),
#                 ),
#             ),
#             mlc.Field(
#                 id="tomogram_segmentation/annotation_id",
#                 name="Annotation ID",
#                 description="ID of the segmentation",
#                 data_types=[mlc.DataType.INTEGER],
#                 references=mlc.Source(
#                     field="annotation_id",
#                 ),
#                 source=mlc.Source(
#                     # field="annotation_id",
#                     file_object="annotations.json",
#                     extract=mlc.Extract(json_path="$[*].id"),
#                 ),
#             ),
#             mlc.Field(
#                 id="tomogram_segmentation/tomogram_s3_omezarr_dir",
#                 name="Tomogram S3 Omezarr Directory",
#                 description="URL to the tomogram OME-Zarr directory",
#                 data_types=[mlc.DataType.URL],
#                 source=mlc.Source(
#                     field="tomogram_s3_omezarr_dir",
#                 ),
#             ),
#             mlc.Field(
#                 id="tomogram_segmentation/annotation_s3_omezarr_dir",
#                 name="Annotation S3 Omezarr Directory",
#                 description="URL to the annotation OME-Zarr directory",
#                 data_types=[mlc.DataType.URL],
#                 source=mlc.Source(
#                     field="annotation_s3_omezarr_dir",
#                 ),
#             ),
#         ],
#     )
#     return rs
#
#
# def run_tomogram_annotations() -> mlc.RecordSet:
#     """
#     Create a join between a tomogram recordset and its segmentation recordset.
#
#     Returns:
#         mlc.RecordSet: The generated join.
#     """
#
#     rs = mlc.RecordSet(
#         id="tomogram_segmentation",
#         name="Tomogram Segmentation",
#         description="Join between tomogram and annotation recordsets that contain segmentation information.",
#         key=["tomogram_segmentation/tomogram_id", "tomogram_segmentation/annotation_id"],
#         fields=[
#             mlc.Field(
#                 id="tomogram_segmentation/tomogram_id",
#                 name="Tomogram ID",
#                 description="ID of the tomogram",
#                 data_types=[mlc.DataType.INTEGER],
#                 references=mlc.Source(
#                     field="tomogram_id",
#                 ),
#                 source=mlc.Source(
#                     # field="tomogram_id",
#                     file_object="tomograms.json",
#                     extract=mlc.Extract(json_path="$[*].id"),
#                 ),
#             ),
#             mlc.Field(
#                 id="tomogram_segmentation/annotation_id",
#                 name="Annotation ID",
#                 description="ID of the segmentation",
#                 data_types=[mlc.DataType.INTEGER],
#                 references=mlc.Source(
#                     field="annotation_id",
#                 ),
#                 source=mlc.Source(
#                     # field="annotation_id",
#                     file_object="annotations.json",
#                     extract=mlc.Extract(json_path="$[*].id"),
#                 ),
#             ),
#             mlc.Field(
#                 id="tomogram_segmentation/tomogram_s3_omezarr_dir",
#                 name="Tomogram S3 Omezarr Directory",
#                 description="URL to the tomogram OME-Zarr directory",
#                 data_types=[mlc.DataType.URL],
#                 source=mlc.Source(
#                     field="tomogram_s3_omezarr_dir",
#                 ),
#             ),
#             mlc.Field(
#                 id="tomogram_segmentation/annotation_s3_omezarr_dir",
#                 name="Annotation S3 Omezarr Directory",
#                 description="URL to the annotation OME-Zarr directory",
#                 data_types=[mlc.DataType.URL],
#                 source=mlc.Source(
#                     field="annotation_s3_omezarr_dir",
#                 ),
#             ),
#         ],
#     )
#     return rs


# def joins() -> list[mlc.RecordSet]:
#     """
#     Create joins between recordsets.
#
#     Returns:
#         list[mlc.RecordSet]: A list of joins.
#     """
#
#     joins = [
#         tomogram_segmentation_join(),
#     ]
#
#     return joins



