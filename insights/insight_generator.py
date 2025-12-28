"""
Insight Generator - AI-Ready Textual Insights
Generates editor-friendly explanations of trends
Following 2025 best practices for explainable analytics
"""
from typing import Dict, Optional
from datetime import datetime


class InsightGenerator:
    """
    Generates textual insights for editorial teams
    Editor-friendly language, no technical jargon
    """
    
    def __init__(self):
        """Initialize insight generator"""
        pass
    
    def generate_trend_insight(
        self,
        comparison_metrics: Dict,
        trend_direction: str,
        date_range: tuple[datetime, datetime],
        seasonality_info: Optional[Dict] = None
    ) -> Dict[str, str]:
        """
        Generate comprehensive trend insight
        
        Args:
            comparison_metrics: Metrics from comparison analysis
            trend_direction: Overall trend direction (growth, decline, stable)
            date_range: Tuple of (start_date, end_date)
            seasonality_info: Optional seasonality information
            
        Returns:
            Dictionary with title, content, and box_type
        """
        pct_change = comparison_metrics["percent_change"]
        comparison_type = comparison_metrics["comparison_type"]
        
        # Determine insight type and messaging
        if trend_direction == "growth":
            box_type = "success"
            title = "✅ Trend in Crescita"
            
            if pct_change > 20:
                strength = "forte"
            elif pct_change > 10:
                strength = "moderata"
            else:
                strength = "leggera"
            
            content = (
                f"Il magazine sta registrando una crescita {strength} delle visualizzazioni. "
                f"Rispetto al periodo precedente ({self._format_comparison_type(comparison_type)}), "
                f"le pagine viste sono aumentate del {pct_change:+.1f}%. "
            )
            
        elif trend_direction == "decline":
            box_type = "warning"
            title = "⚠️ Trend in Calo"
            
            if abs(pct_change) > 20:
                strength = "significativo"
            elif abs(pct_change) > 10:
                strength = "moderato"
            else:
                strength = "lieve"
            
            content = (
                f"Il magazine sta registrando un calo {strength} delle visualizzazioni. "
                f"Rispetto al periodo precedente ({self._format_comparison_type(comparison_type)}), "
                f"le pagine viste sono diminuite del {pct_change:.1f}%. "
            )
            
        else:  # stable
            box_type = "info"
            title = "➡️ Trend Stabile"
            
            content = (
                f"Il magazine mantiene un andamento stabile. "
                f"Rispetto al periodo precedente ({self._format_comparison_type(comparison_type)}), "
                f"la variazione è contenuta ({pct_change:+.1f}%), indicando continuità nelle performance. "
            )
        
        # Add seasonality context if available
        if seasonality_info and seasonality_info.get("has_strong_pattern"):
            strongest_day = seasonality_info.get("strongest_day", "N/A")
            weakest_day = seasonality_info.get("weakest_day", "N/A")
            
            content += (
                f"\n\n📅 Pattern Settimanale: I lettori sono più attivi il {strongest_day}, "
                f"mentre il {weakest_day} è il giorno con meno traffico. "
                f"Considera questa stagionalità per la pianificazione editoriale."
            )
        
        return {
            "title": title,
            "content": content,
            "box_type": box_type
        }
    
    def generate_summary_insight(
        self,
        summary_stats: Dict,
        date_range: tuple[datetime, datetime]
    ) -> str:
        """
        Generate summary insight for the period
        
        Args:
            summary_stats: Summary statistics dictionary
            date_range: Tuple of (start_date, end_date)
            
        Returns:
            Summary text
        """
        start_date, end_date = date_range
        days = summary_stats.get("days_count", 0)
        total_views = summary_stats.get("total_views", 0)
        avg_views = summary_stats.get("daily_average", 0)
        
        return (
            f"Nel periodo analizzato ({start_date.strftime('%d/%m/%Y')} - {end_date.strftime('%d/%m/%Y')}), "
            f"il magazine ha totalizzato {total_views:,.0f} visualizzazioni in {days} giorni, "
            f"con una media giornaliera di {avg_views:,.0f} pagine viste."
        )
    
    def generate_recommendation(
        self,
        trend_direction: str,
        pct_change: float
    ) -> str:
        """
        Generate actionable recommendation based on trend
        
        Args:
            trend_direction: Trend direction
            pct_change: Percentage change
            
        Returns:
            Recommendation text
        """
        if trend_direction == "growth":
            return (
                "💡 Raccomandazione: Il trend positivo suggerisce che le scelte editoriali recenti "
                "stanno funzionando. Analizza i contenuti di maggior successo per replicare i pattern vincenti."
            )
        
        elif trend_direction == "decline":
            return (
                "💡 Raccomandazione: Il calo richiede attenzione. Verifica se è legato a stagionalità "
                "o a un cambio nelle strategie editoriali. Considera di analizzare i contenuti che hanno "
                "performato meglio in passato."
            )
        
        else:  # stable
            return (
                "💡 Raccomandazione: La stabilità è positiva, ma valuta opportunità per stimolare "
                "la crescita attraverso nuovi formati o tematiche che possano attrarre nuovi lettori."
            )
    
    def _format_comparison_type(self, comparison_type: str) -> str:
        """
        Format comparison type for Italian display
        
        Args:
            comparison_type: Raw comparison type (WoW, MoM, YoY)
            
        Returns:
            Formatted Italian text
        """
        formats = {
            "WoW": "settimana precedente",
            "MoM": "mese precedente",
            "YoY": "anno precedente"
        }
        return formats.get(comparison_type, comparison_type)
