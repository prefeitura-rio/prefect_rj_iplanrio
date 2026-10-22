"""PyMongoArrow, imported without its warnings about the size of the Arrow classes.

The import of PyMongoArrow warns that the size of some Arrow classes changed, although
the pyarrow of the environment is in the range that it declares. The classes that the
toolkit uses keep working, and the warnings are noise on every flow start. PyMongoArrow
also pins pyarrow to one minor version. The workspace has one lock, so that version is
the pyarrow of every environment, with the `mongodb` extra or without it.
"""

from warnings import catch_warnings, filterwarnings

with catch_warnings():
    filterwarnings(
        "ignore", message=r"pyarrow\.lib\..* size changed", category=RuntimeWarning
    )
    from pymongoarrow.api import Schema, find_arrow_all
    from pymongoarrow.types import Decimal128Type, ObjectIdType

__all__ = ["Decimal128Type", "ObjectIdType", "Schema", "find_arrow_all"]
