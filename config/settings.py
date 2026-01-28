"""
Dashboard Configuration Settings
Following 2025 best practices for configuration management
"""
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class GA4Config:
    """Google Analytics 4 configuration"""
    property_id: str = os.getenv("GA4_PROPERTY_ID", "YOUR_GA4_PROPERTY_ID")
    credentials_path: str = os.getenv(
        "GA4_CREDENTIALS_PATH", 
        str(Path(__file__).parent.parent.parent / "API" / "client_secret_8010833880-k35t6cr6lg0uca30fle9ib254ohm0tcb.apps.googleusercontent.com.json")
    )


@dataclass
class DashboardConfig:
    """Dashboard application configuration"""
    app_title: str = "Editorial Analytics Dashboard"
    debug_mode: bool = os.getenv("DEBUG", "False").lower() == "true"
    host: str = os.getenv("HOST", "127.0.0.1")
    port: int = int(os.getenv("PORT", "8050"))
    
    # UI Configuration
    default_date_range_days: int = 90
    default_granularity: str = "daily"
    
    # Analytics Configuration
    moving_average_windows: dict = None
    comparison_periods: list = None
    
    def __post_init__(self):
        if self.moving_average_windows is None:
            self.moving_average_windows = {
                "7d": 7,
                "14d": 14,
                "30d": 30
            }
        if self.comparison_periods is None:
            self.comparison_periods = ["WoW", "MoM", "YoY"]


@dataclass
class MetricsConfig:
    """Metrics and dimensions configuration"""
    primary_metric: str = "screenPageViews"
    secondary_metrics: list = None
    dimensions: list = None
    
    def __post_init__(self):
        if self.secondary_metrics is None:
            self.secondary_metrics = ["activeUsers", "sessions"]
        if self.dimensions is None:
            self.dimensions = ["date"]


# Singleton instances
ga4_config = GA4Config()
dashboard_config = DashboardConfig()
metrics_config = MetricsConfig()
