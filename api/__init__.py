"""Flask application package initialization."""

__all__ = ["create_app"]


def __getattr__(name):
    # Lazy import to avoid double-loading when running `python -m api.app`
    if name == "create_app":
        from .app import create_app as _create_app

        return _create_app
    raise AttributeError(f"module {__name__} has no attribute {name}")
