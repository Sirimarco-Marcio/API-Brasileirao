"""Data acquisition package."""

__all__ = ["get_connection", "init_db"]


def __getattr__(name):
    # Lazy import to avoid double-loading when running `python -m data_acquisition.db`
    if name in __all__:
        from . import db as _db

        return getattr(_db, name)
    raise AttributeError(f"module {__name__} has no attribute {name}")
