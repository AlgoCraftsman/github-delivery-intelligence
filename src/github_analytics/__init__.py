"""GitHub delivery intelligence application package."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("github-delivery-intelligence")
except PackageNotFoundError:
    __version__ = "0+source"

__all__ = ["__version__"]
