"""
Dashboard Version Information
Follows Semantic Versioning 2.0.0 (https://semver.org/)

Version format: MAJOR.MINOR.PATCH
- MAJOR: Breaking changes
- MINOR: New features (backward compatible)
- PATCH: Bug fixes (backward compatible)
"""

__version__ = "1.0.0"
__version_info__ = (1, 0, 0)

# Release metadata
__title__ = "Editorial Analytics Dashboard"
__description__ = "Production-ready analytics dashboard for editorial teams"
__author__ = "Havas Analytics Team"
__license__ = "MIT"

# Build metadata (optional, can be updated by CI/CD)
__build__ = None
__commit__ = None

# API version (useful for breaking changes)
API_VERSION = "v1"

# Feature flags for this version
FEATURES = {
    "trend_analysis": True,
    "period_comparison": True,
    "seasonality_detection": True,
    "ai_insights": True,
    "caching": True,
    "export_data": False,  # Future feature
    "custom_metrics": False,  # Future feature
}

# Changelog for this version
CHANGELOG = """
Version 1.0.0 (2025-12-28)
--------------------------
Initial release

Features:
- GA4 integration with Facade pattern
- Trend analysis with multiple time granularities
- Period comparison (WoW, MoM, YoY)
- Seasonality detection
- AI-ready insights generation
- Cached data access
- Editor-friendly UI
- Object-oriented architecture

Architecture:
- Repository pattern for data access
- Service layer for business logic
- Strategy pattern for time aggregation
- MVC-inspired callback structure
"""


def get_version() -> str:
    """Get version string"""
    return __version__


def get_version_info() -> tuple:
    """Get version as tuple"""
    return __version_info__


def get_full_version() -> str:
    """Get full version with build info if available"""
    version = __version__
    if __build__:
        version += f"+{__build__}"
    return version


def check_feature(feature_name: str) -> bool:
    """
    Check if a feature is enabled in this version
    
    Args:
        feature_name: Name of the feature to check
        
    Returns:
        True if feature is enabled, False otherwise
    """
    return FEATURES.get(feature_name, False)
