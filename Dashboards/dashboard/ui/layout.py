"""
Dashboard Layout - Main UI Structure
Following MVC-inspired separation for Dash
2025 best practices: clean, modular, editor-friendly
"""
from dash import html, dcc
from datetime import datetime, timedelta
from .components import ComponentFactory


def create_layout() -> html.Div:
    """
    Create main dashboard layout
    
    Returns:
        Dash HTML layout component
    """
    # Default date range: last 90 days
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=90)
    
    return html.Div([
        # Header
        html.Div([
            html.H1(
                "📊 Editorial Analytics Dashboard",
                style={
                    "color": "#fff",
                    "margin": "0",
                    "fontSize": "28px",
                    "fontWeight": "600"
                }
            ),
            html.P(
                "Analisi del trend delle visualizzazioni • Taxi Drivers Magazine",
                style={
                    "color": "#e0e0e0",
                    "margin": "8px 0 0 0",
                    "fontSize": "14px"
                }
            )
        ], style={
            "backgroundColor": "#1976D2",
            "padding": "30px 40px",
            "marginBottom": "30px",
            "boxShadow": "0 2px 4px rgba(0,0,0,0.1)"
        }),
        
        # Main content container
        html.Div([
            # Controls section
            html.Div([
                html.H3(
                    "⚙️ Controlli",
                    style={
                        "color": "#333",
                        "marginBottom": "20px",
                        "fontSize": "20px",
                        "fontWeight": "600"
                    }
                ),
                
                # Date range picker
                ComponentFactory.create_date_range_selector("date-range-picker"),
                
                # Granularity selector
                ComponentFactory.create_granularity_selector("granularity-selector"),
                
                # Comparison selector
                ComponentFactory.create_comparison_selector("comparison-selector"),
                
                # Update button
                html.Button(
                    "🔄 Aggiorna Dati",
                    id="update-button",
                    n_clicks=0,
                    style={
                        "width": "100%",
                        "padding": "12px",
                        "backgroundColor": "#1976D2",
                        "color": "white",
                        "border": "none",
                        "borderRadius": "4px",
                        "fontSize": "14px",
                        "fontWeight": "600",
                        "cursor": "pointer",
                        "marginTop": "10px"
                    }
                )
            ], style={
                "backgroundColor": "#fff",
                "padding": "25px",
                "borderRadius": "8px",
                "boxShadow": "0 2px 4px rgba(0,0,0,0.1)",
                "marginBottom": "30px"
            }),
            
            # Key metrics section
            html.Div([
                html.H3(
                    "📈 Metriche Chiave",
                    style={
                        "color": "#333",
                        "marginBottom": "20px",
                        "fontSize": "20px",
                        "fontWeight": "600"
                    }
                ),
                html.Div(id="metrics-cards", children=[
                    # Placeholder - will be populated by callback
                    html.P("Caricamento metriche...", style={"color": "#999"})
                ])
            ], style={
                "marginBottom": "30px"
            }),
            
            # AI Insight section
            html.Div([
                html.H3(
                    "💡 Insight Automatico",
                    style={
                        "color": "#333",
                        "marginBottom": "20px",
                        "fontSize": "20px",
                        "fontWeight": "600"
                    }
                ),
                html.Div(id="insight-box", children=[
                    # Placeholder - will be populated by callback
                    ComponentFactory.create_insight_box(
                        title="Analisi del Trend",
                        content="Seleziona un periodo e clicca 'Aggiorna Dati' per vedere l'analisi automatica.",
                        box_type="info"
                    )
                ])
            ], style={
                "marginBottom": "30px"
            }),
            
            # Main trend chart
            html.Div([
                html.H3(
                    "📉 Trend delle Visualizzazioni",
                    style={
                        "color": "#333",
                        "marginBottom": "20px",
                        "fontSize": "20px",
                        "fontWeight": "600"
                    }
                ),
                dcc.Loading(
                    id="loading-trend",
                    type="default",
                    children=dcc.Graph(id="trend-chart")
                )
            ], style={
                "backgroundColor": "#fff",
                "padding": "25px",
                "borderRadius": "8px",
                "boxShadow": "0 2px 4px rgba(0,0,0,0.1)",
                "marginBottom": "30px"
            }),
            
            # Seasonality analysis
            html.Div([
                html.H3(
                    "🔄 Analisi Stagionalità",
                    style={
                        "color": "#333",
                        "marginBottom": "20px",
                        "fontSize": "20px",
                        "fontWeight": "600"
                    }
                ),
                dcc.Loading(
                    id="loading-seasonality",
                    type="default",
                    children=dcc.Graph(id="seasonality-chart")
                )
            ], style={
                "backgroundColor": "#fff",
                "padding": "25px",
                "borderRadius": "8px",
                "boxShadow": "0 2px 4px rgba(0,0,0,0.1)",
                "marginBottom": "30px"
            }),
            
            # Footer
            html.Div([
                html.P([
                    "📊 Dati da Google Analytics 4 • ",
                    html.Span(id="last-update", children="Ultimo aggiornamento: in attesa..."),
                    " • Dashboard v1.0"
                ], style={
                    "color": "#999",
                    "fontSize": "12px",
                    "margin": "0",
                    "textAlign": "center"
                })
            ], style={
                "padding": "20px",
                "borderTop": "1px solid #eee",
                "marginTop": "30px"
            })
            
        ], style={
            "maxWidth": "1400px",
            "margin": "0 auto",
            "padding": "0 40px 40px 40px"
        })
        
    ], style={
        "backgroundColor": "#f5f5f5",
        "minHeight": "100vh",
        "fontFamily": "Arial, Helvetica, sans-serif"
    })
