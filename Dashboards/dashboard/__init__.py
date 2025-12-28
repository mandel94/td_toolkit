"""
Editorial Analytics Dashboard Package
2025 Best Practices Implementation
"""

from .__version__ import (
    __version__,
    __version_info__,
    __title__,
    __description__,
    __author__,
    __license__,
    API_VERSION,
    get_version,
    get_version_info,
    get_full_version,
    check_feature,
)

from .app import create_app, main

__all__ = [
    # App
    "create_app",
    "main",
    # Version info
    "__version__",
    "__version_info__",
    "__title__",
    "__description__",
    "__author__",
    "__license__",
    "API_VERSION",
    "get_version",
    "get_version_info",
    "get_full_version",
    "check_feature",
]
