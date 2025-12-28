"""
Editorial Analytics Dashboard
Entry point for the Dash application

Following 2025 best practices:
- Object-oriented design
- Clean architecture
- Separation of concerns
- Editor-friendly interface
"""
import dash
from dash import Dash

from config import ga4_config, dashboard_config
from data import CachedAnalyticsRepository
from services import AnalyticsService, TrendService
from ui import create_layout
from ui.callbacks import register_callbacks
from __version__ import __version__, __title__


def create_app() -> Dash:
    """
    Create and configure Dash application
    
    Returns:
        Configured Dash app instance
    """
    # Initialize Dash app
    app = Dash(
        __name__,
        title=dashboard_config.app_title,
        suppress_callback_exceptions=True
    )
    
    # Initialize services (Dependency Injection pattern)
    repository = CachedAnalyticsRepository()
    analytics_service = AnalyticsService(repository)
    trend_service = TrendService()
    
    # Set layout
    app.layout = create_layout()
    
    # Register callbacks
    register_callbacks(app, analytics_service, trend_service)
    
    return app


def main():
    """
    Main entry point
    """
    app = create_app()
    
    print(f"""
    ╔══════════════════════════════════════════════════════════════╗
    ║  📊 {__title__:<55} ║
    ║  Version {__version__:<50} ║
    ║  Taxi Drivers Magazine - 2025                              ║
    ╚══════════════════════════════════════════════════════════════╝
    
    🚀 Starting dashboard server...
    🌐 URL: http://{dashboard_config.host}:{dashboard_config.port}
    📁 GA4 Property: {ga4_config.property_id}
    
    Press CTRL+C to stop
    """)
    
    app.run_server(
        debug=dashboard_config.debug_mode,
        host=dashboard_config.host,
        port=dashboard_config.port
    )


if __name__ == "__main__":
    main()
