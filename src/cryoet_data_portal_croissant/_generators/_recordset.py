from typing import Optional, Type

import cryoet_data_portal as cdp
from pydantic import BaseModel, create_model

_portal_types = (
    Type[cdp.Annotation]
    | Type[cdp.AnnotationShape]
    | Type[cdp.AnnotationFile]
    | Type[cdp.Tomogram]
    | Type[cdp.TiltSeries]
    | Type[cdp.Dataset]
    | Type[cdp.Run]
    | Type[cdp.Alignment]
)


def _portal_to_model(clz: _portal_types, name: str) -> Type[BaseModel]:
    """Automatically create a Pydantic model from a CryoET Data Portal annotation class."""
    vals = clz.__annotations__
    scalars = {k: (Optional[v], None) for k, v in vals.items() if v in ["int", "float", "str", "bool"] and k[0] != "_"}
    return create_model(name, **scalars)
