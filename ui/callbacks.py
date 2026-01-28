"""
Dashboard Callbacks - Interactivity Logic
Implements Dash callbacks following MVC-inspired pattern
2025 best practices: clean separation of concerns
"""
from dash import Input, Output, State, html
from datetime import datetime
import pandas as pd
import plotly.graph_objects as go

from services import AnalyticsService, TrendService
from data import CachedAnalyticsRepository
from ui.components import ComponentFactory, ChartFactory
from insights import InsightGenerator


def register_callbacks(app, analytics_service: AnalyticsService, trend_service: TrendService):
    """
    Register all dashboard callbacks
    
    Args:
        app: Dash application instance
        analytics_service: Analytics service instance
        trend_service: Trend service instance
    """
    insight_generator = InsightGenerator()
    
    @app.callback(
        [
            Output("metrics-cards", "children"),
            Output("insight-box", "children"),
            Output("trend-chart", "figure"),
            Output("seasonality-chart", "figure"),
            Output("last-update", "children")
        ],
        [
            Input("update-button", "n_clicks")
        ],
        [
            State("date-range-picker", "start_date"),
            State("date-range-picker", "end_date"),
            State("granularity-selector", "value"),
            State("comparison-selector", "value")
        ]
    )
    def update_dashboard(n_clicks, start_date, end_date, granularity, comparison_type):
        """
        Main callback to update entire dashboard
        
        Args:
            n_clicks: Number of button clicks
            start_date: Selected start date
            end_date: Selected end date
            granularity: Selected granularity
            comparison_type: Selected comparison type
            
        Returns:
            Tuple of updated components
        """
        # Handle initial load or invalid dates
        if start_date is None or end_date is None:
            from datetime import timedelta
            end = datetime.now()
            start = end - timedelta(days=90)
        else:
            start = datetime.fromisoformat(start_date)
            end = datetime.fromisoformat(end_date)
        
        try:
            # Fetch and process data
            df = analytics_service.get_trend_data(start, end, granularity)
            
            if df.empty:
                return (
                    [html.P("Nessun dato disponibile per il periodo selezionato.", 
                           style={"color": "#999"})],
                    [ComponentFactory.create_insight_box(
                        title="Dati non disponibili",
                        content="Non ci sono dati per il periodo selezionato.",
                        box_type="warning"
                    )],
                    go.Figure(),
                    go.Figure(),
                    f"Ultimo aggiornamento: {datetime.now().strftime('%d/%m/%Y %H:%M')}"
                )
            
            # Get comparison metrics
            comparison_metrics = analytics_service.get_comparison_metrics(
                start, end, comparison_type
            )
            
            # Get summary statistics
            summary_stats = analytics_service.get_date_range_summary(start, end)
            
            # Add moving averages
            df_with_ma = trend_service.add_moving_averages(df)
            
            # Detect trend direction
            trend_direction = trend_service.detect_trend_direction(df)
            
            # Get seasonality info
            seasonality_info = trend_service.identify_seasonality_pattern(df)
            
            # Generate metrics cards
            metrics_cards = create_metrics_cards(
                summary_stats,
                comparison_metrics,
                trend_direction
            )
            
            # Generate insight
            insight_data = insight_generator.generate_trend_insight(
                comparison_metrics,
                trend_direction,
                (start, end),
                seasonality_info
            )
            
            insight_box = ComponentFactory.create_insight_box(
                title=insight_data["title"],
                content=insight_data["content"],
                box_type=insight_data["box_type"]
            )
            
            # Create trend chart
            trend_chart = ChartFactory.create_trend_line_chart(
                df_with_ma,
                x_column="date",
                y_columns=["screenPageViews", "screenPageViews_ma_7d"],
                title=f"Trend delle Visualizzazioni ({granularity.capitalize()})"
            )
            
            # Create seasonality chart
            seasonality_chart = create_seasonality_chart(seasonality_info)
            
            # Update timestamp
            update_time = f"Ultimo aggiornamento: {datetime.now().strftime('%d/%m/%Y %H:%M')}"
            
            return metrics_cards, [insight_box], trend_chart, seasonality_chart, update_time
            
        except Exception as e:
            error_msg = f"Errore nel caricamento dei dati: {str(e)}"
            return (
                [html.P(error_msg, style={"color": "#f44336"})],
                [ComponentFactory.create_insight_box(
                    title="Errore",
                    content=error_msg,
                    box_type="danger"
                )],
                go.Figure(),
                go.Figure(),
                f"Errore: {datetime.now().strftime('%d/%m/%Y %H:%M')}"
            )
    
    @app.callback(
        Output("date-range-picker", "start_date"),
        Output("date-range-picker", "end_date"),
        Input("date-range-picker", "id")
    )
    def set_default_dates(_):
        """
        Set default date range on load
        
        Returns:
            Default start and end dates
        """
        from datetime import timedelta
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=90)
        return start_date, end_date


def create_metrics_cards(summary_stats, comparison_metrics, trend_direction):
    """
    Create metrics cards layout
    
    Args:
        summary_stats: Summary statistics dictionary
        comparison_metrics: Comparison metrics dictionary
        trend_direction: Trend direction string
        
    Returns:
        List of metric card components
    """
    trend_symbols = {
        "growth": "↑",
        "decline": "↓",
        "stable": "→"
    }
    
    cards = html.Div([
        html.Div([
            ComponentFactory.create_metric_card(
                title="Visualizzazioni Totali",
                value=f"{summary_stats['total_views']:,.0f}",
                subtitle=f"in {summary_stats['days_count']} giorni",
                trend_indicator=trend_symbols.get(trend_direction),
                color="#2196F3"
            )
        ], style={"flex": "1", "minWidth": "250px"}),
        
        html.Div([
            ComponentFactory.create_metric_card(
                title="Media Giornaliera",
                value=f"{summary_stats['daily_average']:,.0f}",
                subtitle="visualizzazioni/giorno",
                color="#4caf50"
            )
        ], style={"flex": "1", "minWidth": "250px"}),
        
        html.Div([
            ComponentFactory.create_metric_card(
                title=f"Variazione {comparison_metrics['comparison_type']}",
                value=f"{comparison_metrics['percent_change']:+.1f}%",
                subtitle=f"{comparison_metrics['absolute_change']:+,.0f} visualizzazioni",
                trend_indicator=trend_symbols.get(comparison_metrics['direction']),
                color="#ff9800"
            )
        ], style={"flex": "1", "minWidth": "250px"}),
        
        html.Div([
            ComponentFactory.create_metric_card(
                title="Trend",
                value={"growth": "Crescita", "decline": "Calo", "stable": "Stabile"}[trend_direction],
                subtitle="direzione generale",
                trend_indicator=trend_symbols.get(trend_direction),
                color="#9c27b0"
            )
        ], style={"flex": "1", "minWidth": "250px"})
        
    ], style={
        "display": "flex",
        "gap": "20px",
        "flexWrap": "wrap"
    })
    
    return cards


def create_seasonality_chart(seasonality_info):
    """
    Create seasonality pattern chart
    
    Args:
        seasonality_info: Seasonality information dictionary
        
    Returns:
        Plotly Figure object
    """
    weekday_avg = seasonality_info.get("weekday_averages", {})
    
    if not weekday_avg:
        return go.Figure()
    
    # Order weekdays
    weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 
                    'Friday', 'Saturday', 'Sunday']
    italian_names = ['Lunedì', 'Martedì', 'Mercoledì', 'Giovedì', 
                    'Venerdì', 'Sabato', 'Domenica']
    
    ordered_values = []
    ordered_labels = []
    
    for eng_day, ita_day in zip(weekday_order, italian_names):
        if eng_day in weekday_avg:
            ordered_values.append(weekday_avg[eng_day])
            ordered_labels.append(ita_day)
    
    # Create bar chart
    colors = ['#2196F3' if day != seasonality_info.get('strongest_day') 
              else '#4caf50' for day in weekday_order if day in weekday_avg]
    
    fig = go.Figure(data=[
        go.Bar(
            x=ordered_labels,
            y=ordered_values,
            marker_color=colors,
            text=[f"{v:,.0f}" for v in ordered_values],
            textposition='outside'
        )
    ])
    
    fig.update_layout(
        title=dict(
            text="Pattern Settimanale delle Visualizzazioni",
            font=dict(size=18, color="#333")
        ),
        xaxis_title="Giorno della Settimana",
        yaxis_title="Media Visualizzazioni",
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(family="Arial, sans-serif", size=12, color="#666"),
        showlegend=False,
        margin=dict(l=60, r=40, t=60, b=60)
    )
    
    fig.update_xaxes(showgrid=False, showline=True, linewidth=1, linecolor="#ddd")
    fig.update_yaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor="#f0f0f0",
        showline=True,
        linewidth=1,
        linecolor="#ddd"
    )
    
    return fig
