"""
UI Components - Reusable Dashboard Components
Following component-based architecture (2025 best practices)
"""
from typing import Optional, List
import plotly.graph_objects as go
from dash import html, dcc
import pandas as pd


class ComponentFactory:
    """
    Factory for creating dashboard UI components
    Following Factory pattern for UI creation
    """
    
    @staticmethod
    def create_metric_card(
        title: str,
        value: str,
        subtitle: Optional[str] = None,
        trend_indicator: Optional[str] = None,
        color: str = "#1f77b4"
    ) -> html.Div:
        """
        Create a metric card component
        
        Args:
            title: Card title
            value: Main value to display
            subtitle: Optional subtitle
            trend_indicator: Optional trend indicator (↑, →, ↓)
            color: Card accent color
            
        Returns:
            Dash HTML component
        """
        trend_colors = {
            "↑": "#28a745",
            "→": "#ffc107",
            "↓": "#dc3545"
        }
        
        children = [
            html.H4(title, style={"color": "#666", "fontSize": "14px", "marginBottom": "8px"}),
            html.Div([
                html.Span(value, style={"fontSize": "32px", "fontWeight": "bold", "color": color}),
                html.Span(
                    f" {trend_indicator}" if trend_indicator else "",
                    style={
                        "fontSize": "24px",
                        "marginLeft": "8px",
                        "color": trend_colors.get(trend_indicator, "#666")
                    }
                ) if trend_indicator else html.Span()
            ])
        ]
        
        if subtitle:
            children.append(
                html.P(subtitle, style={"color": "#999", "fontSize": "12px", "marginTop": "8px"})
            )
        
        return html.Div(
            children=children,
            style={
                "padding": "20px",
                "backgroundColor": "#fff",
                "borderRadius": "8px",
                "boxShadow": "0 2px 4px rgba(0,0,0,0.1)",
                "border": f"2px solid {color}",
                "minHeight": "120px"
            }
        )
    
    @staticmethod
    def create_insight_box(
        title: str,
        content: str,
        box_type: str = "info"
    ) -> html.Div:
        """
        Create an insight box component
        
        Args:
            title: Box title
            content: Insight text
            box_type: Type of box (info, success, warning, danger)
            
        Returns:
            Dash HTML component
        """
        colors = {
            "info": {"bg": "#e7f3ff", "border": "#2196F3", "text": "#0d47a1"},
            "success": {"bg": "#e8f5e9", "border": "#4caf50", "text": "#1b5e20"},
            "warning": {"bg": "#fff3e0", "border": "#ff9800", "text": "#e65100"},
            "danger": {"bg": "#ffebee", "border": "#f44336", "text": "#b71c1c"}
        }
        
        style_config = colors.get(box_type, colors["info"])
        
        return html.Div([
            html.H4(
                title,
                style={
                    "color": style_config["text"],
                    "fontSize": "16px",
                    "marginBottom": "12px",
                    "fontWeight": "600"
                }
            ),
            html.P(
                content,
                style={
                    "color": style_config["text"],
                    "fontSize": "14px",
                    "lineHeight": "1.6",
                    "margin": "0"
                }
            )
        ], style={
            "padding": "20px",
            "backgroundColor": style_config["bg"],
            "borderLeft": f"4px solid {style_config['border']}",
            "borderRadius": "4px",
            "marginBottom": "20px"
        })
    
    @staticmethod
    def create_date_range_selector(
        component_id: str = "date-range-picker"
    ) -> html.Div:
        """
        Create date range selector component
        
        Args:
            component_id: Component ID for callbacks
            
        Returns:
            Dash HTML component
        """
        return html.Div([
            html.Label(
                "Seleziona Periodo:",
                style={"fontWeight": "600", "marginBottom": "8px", "display": "block"}
            ),
            dcc.DatePickerRange(
                id=component_id,
                display_format='DD/MM/YYYY',
                style={"marginBottom": "10px"}
            )
        ], style={"marginBottom": "20px"})
    
    @staticmethod
    def create_granularity_selector(
        component_id: str = "granularity-selector"
    ) -> html.Div:
        """
        Create granularity selector component
        
        Args:
            component_id: Component ID for callbacks
            
        Returns:
            Dash HTML component
        """
        return html.Div([
            html.Label(
                "Granularità:",
                style={"fontWeight": "600", "marginBottom": "8px", "display": "block"}
            ),
            dcc.RadioItems(
                id=component_id,
                options=[
                    {"label": "Giornaliera", "value": "daily"},
                    {"label": "Settimanale", "value": "weekly"},
                    {"label": "Mensile", "value": "monthly"}
                ],
                value="daily",
                inline=True,
                style={"marginBottom": "10px"}
            )
        ], style={"marginBottom": "20px"})
    
    @staticmethod
    def create_comparison_selector(
        component_id: str = "comparison-selector"
    ) -> html.Div:
        """
        Create comparison period selector
        
        Args:
            component_id: Component ID for callbacks
            
        Returns:
            Dash HTML component
        """
        return html.Div([
            html.Label(
                "Confronto con:",
                style={"fontWeight": "600", "marginBottom": "8px", "display": "block"}
            ),
            dcc.Dropdown(
                id=component_id,
                options=[
                    {"label": "Settimana precedente (WoW)", "value": "WoW"},
                    {"label": "Mese precedente (MoM)", "value": "MoM"},
                    {"label": "Anno precedente (YoY)", "value": "YoY"}
                ],
                value="WoW",
                clearable=False,
                style={"marginBottom": "10px"}
            )
        ], style={"marginBottom": "20px"})


class ChartFactory:
    """
    Factory for creating Plotly charts
    Following 2025 visualization best practices
    """
    
    @staticmethod
    def create_trend_line_chart(
        df: pd.DataFrame,
        x_column: str = "date",
        y_columns: List[str] = None,
        title: str = "Page Views Trend",
        show_legend: bool = True
    ) -> go.Figure:
        """
        Create a line chart for trend visualization
        
        Args:
            df: DataFrame with data
            x_column: Column for x-axis
            y_columns: List of columns for y-axis
            title: Chart title
            show_legend: Whether to show legend
            
        Returns:
            Plotly Figure object
        """
        if y_columns is None:
            y_columns = ["screenPageViews"]
        
        fig = go.Figure()
        
        colors = ["#2196F3", "#4caf50", "#ff9800"]
        
        for idx, y_col in enumerate(y_columns):
            if y_col in df.columns:
                fig.add_trace(go.Scatter(
                    x=df[x_column],
                    y=df[y_col],
                    mode='lines',
                    name=y_col,
                    line=dict(color=colors[idx % len(colors)], width=2)
                ))
        
        fig.update_layout(
            title=dict(text=title, font=dict(size=20, color="#333")),
            xaxis_title="Data",
            yaxis_title="Visualizzazioni",
            plot_bgcolor="white",
            paper_bgcolor="white",
            font=dict(family="Arial, sans-serif", size=12, color="#666"),
            hovermode='x unified',
            showlegend=show_legend,
            margin=dict(l=60, r=40, t=80, b=60)
        )
        
        fig.update_xaxes(
            showgrid=True,
            gridwidth=1,
            gridcolor="#f0f0f0",
            showline=True,
            linewidth=1,
            linecolor="#ddd"
        )
        
        fig.update_yaxes(
            showgrid=True,
            gridwidth=1,
            gridcolor="#f0f0f0",
            showline=True,
            linewidth=1,
            linecolor="#ddd"
        )
        
        return fig
    
    @staticmethod
    def create_comparison_bar_chart(
        categories: List[str],
        values: List[float],
        title: str = "Comparison"
    ) -> go.Figure:
        """
        Create a bar chart for comparisons
        
        Args:
            categories: Category labels
            values: Values for each category
            title: Chart title
            
        Returns:
            Plotly Figure object
        """
        colors = ["#2196F3" if v >= 0 else "#f44336" for v in values]
        
        fig = go.Figure(data=[
            go.Bar(
                x=categories,
                y=values,
                marker_color=colors,
                text=[f"{v:+.1f}%" for v in values],
                textposition='outside'
            )
        ])
        
        fig.update_layout(
            title=dict(text=title, font=dict(size=20, color="#333")),
            plot_bgcolor="white",
            paper_bgcolor="white",
            font=dict(family="Arial, sans-serif", size=12, color="#666"),
            showlegend=False,
            margin=dict(l=60, r=40, t=80, b=60)
        )
        
        fig.update_xaxes(showgrid=False, showline=True, linewidth=1, linecolor="#ddd")
        fig.update_yaxes(
            showgrid=True,
            gridwidth=1,
            gridcolor="#f0f0f0",
            showline=True,
            linewidth=1,
            linecolor="#ddd",
            title="Variazione %"
        )
        
        return fig
    
    @staticmethod
    def create_dual_axis_line_chart(
        df: pd.DataFrame,
        x_column: str = "date",
        y1_column: str = "newUsers",
        y2_column: str = "returningUsers",
        y1_label: str = "Nuovi Utenti",
        y2_label: str = "Utenti di Ritorno",
        title: str = "Fidelizzazione Utenti"
    ) -> go.Figure:
        """
        Create a line chart with dual Y-axes
        
        Args:
            df: DataFrame with data
            x_column: Column for x-axis
            y1_column: Column for first y-axis (left)
            y2_column: Column for second y-axis (right)
            y1_label: Label for first y-axis
            y2_label: Label for second y-axis
            title: Chart title
            
        Returns:
            Plotly Figure object with dual axes
        """
        fig = go.Figure()
        
        # Add first trace (new users) on left Y-axis
        fig.add_trace(go.Scatter(
            x=df[x_column],
            y=df[y1_column],
            mode='lines',
            name=y1_label,
            line=dict(color="#2196F3", width=2),
            yaxis='y1'
        ))
        
        # Add second trace (returning users) on right Y-axis
        fig.add_trace(go.Scatter(
            x=df[x_column],
            y=df[y2_column],
            mode='lines',
            name=y2_label,
            line=dict(color="#4caf50", width=2),
            yaxis='y2'
        ))
        
        # Update layout with dual axes
        fig.update_layout(
            title=dict(text=title, font=dict(size=20, color="#333")),
            xaxis=dict(
                title="Data",
                showgrid=True,
                gridwidth=1,
                gridcolor="#f0f0f0",
                showline=True,
                linewidth=1,
                linecolor="#ddd"
            ),
            yaxis=dict(
                title=y1_label,
                titlefont=dict(color="#2196F3"),
                tickfont=dict(color="#2196F3"),
                showgrid=True,
                gridwidth=1,
                gridcolor="#f0f0f0",
                showline=True,
                linewidth=1,
                linecolor="#ddd"
            ),
            yaxis2=dict(
                title=y2_label,
                titlefont=dict(color="#4caf50"),
                tickfont=dict(color="#4caf50"),
                overlaying='y',
                side='right',
                showgrid=False,
                showline=True,
                linewidth=1,
                linecolor="#ddd"
            ),
            plot_bgcolor="white",
            paper_bgcolor="white",
            font=dict(family="Arial, sans-serif", size=12, color="#666"),
            hovermode='x unified',
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            margin=dict(l=60, r=60, t=80, b=60)
        )
        
        return fig
