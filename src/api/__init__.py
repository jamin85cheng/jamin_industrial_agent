"""API package exports.

Keep this package side-effect light so repository and bootstrap modules can
import `src.api.*` submodules without forcing `src.api.main` during module
initialization.
"""

from importlib import import_module

__version__ = "v1.0.0-beta2"
__codename__ = ""

__all__ = ["app"]


def __getattr__(name: str):
    if name == "app":
        return import_module("src.api.main").app
    raise AttributeError(f"module 'src.api' has no attribute {name!r}")
