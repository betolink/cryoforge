from .ingestitem import ingest_item, ingest_stac
from .search_items import search_items

__all__ = [
    "generate_itslive_metadata",
    "save_metadata",
    "create_stac_item",
    "ingest_item",
    "ingest_stac",
    "generate_items",
    "search_items",
    "generate_items_from_parquet",
]


def __getattr__(name):
    """Lazy import modules with heavy dependencies (kerchunk, h5py, etc)."""
    if name == "generate_itslive_metadata" or name == "save_metadata" or name == "create_stac_item":
        from .generate import generate_itslive_metadata, save_metadata, create_stac_item
        return locals()[name]
    elif name == "generate_items":
        from .generatebulk import generate_items
        return generate_items
    elif name == "generate_items_from_parquet":
        from .generatebatched import process_row_group as generate_items_from_parquet
        return generate_items_from_parquet
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
