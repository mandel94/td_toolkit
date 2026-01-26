"""
GA4 Performance Comparison Tool
Estrae metriche chiave da Google Analytics 4 e le confronta tra due periodi.
"""
import sys
import os
from datetime import datetime, timedelta, date
from typing import Dict, Tuple, Optional, List
from dataclasses import dataclass
from enum import Enum
import pandas as pd

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ga4_api.ga4_api import Ga4Client


class MetricType(Enum):
    """Tipi di metriche disponibili"""
    VIEWS = "screenPageViews"
    USERS = "activeUsers"
    SESSION_DURATION = "averageSessionDuration"
    ENGAGEMENT_RATE = "engagementRate"


@dataclass
class PeriodMetrics:
    """Metriche per un singolo periodo"""
    views: int
    users: int
    avg_session_duration: float  # in secondi
    engagement_rate: float  # come decimale (0-1)
    start_date: str
    end_date: str
    
    def format_duration(self) -> str:
        """Formatta la durata in formato mm:ss"""
        minutes = int(self.avg_session_duration // 60)
        seconds = int(self.avg_session_duration % 60)
        return f"{minutes}m {seconds:02d}s"
    
    def format_engagement_rate(self) -> str:
        """Formatta il tasso di engagement come percentuale"""
        return f"{self.engagement_rate * 100:.1f}%"


@dataclass
class MetricComparison:
    """Confronto tra due periodi per una metrica"""
    current_value: float
    previous_value: float
    metric_name: str
    
    @property
    def absolute_change(self) -> float:
        """Cambio assoluto"""
        return self.current_value - self.previous_value
    
    @property
    def percentage_change(self) -> float:
        """Cambio percentuale"""
        if self.previous_value == 0:
            return 0.0
        return ((self.current_value - self.previous_value) / self.previous_value) * 100
    
    def format_change(self, show_unchanged_label: bool = True) -> str:
        """
        Formatta il cambio come stringa con segno
        
        Args:
            show_unchanged_label: Se True, mostra "sostanzialmente invariato" per cambi < 1%
                                  Se False, mostra sempre la percentuale
        """
        change = self.percentage_change
        if abs(change) < 1 and show_unchanged_label:
            return "sostanzialmente invariato"
        sign = "+" if change > 0 else ""
        return f"{sign}{change:.0f}%"


class GA4PerformanceAnalyzer:
    """Analizzatore di performance GA4 con confronto tra periodi"""
    
    def __init__(self, property_id: str = "394327334"):
        """
        Inizializza l'analizzatore
        
        Args:
            property_id: ID della property GA4
        """
        self.property_id = property_id
        self.client = Ga4Client()
    
    def get_period_metrics(
        self,
        start_date: str,
        end_date: str
    ) -> PeriodMetrics:
        """
        Estrae metriche per un periodo specifico
        
        Args:
            start_date: Data inizio in formato YYYY-MM-DD
            end_date: Data fine in formato YYYY-MM-DD
            
        Returns:
            PeriodMetrics con i dati del periodo
        """
        # Metriche da estrarre
        metrics = [
            MetricType.VIEWS.value,
            MetricType.USERS.value,
            MetricType.SESSION_DURATION.value,
            MetricType.ENGAGEMENT_RATE.value
        ]
        
        # Esegui query GA4
        df = self.client.run_query(
            property_id=self.property_id,
            dimensions=[],  # Nessuna dimensione, solo totali
            metrics=metrics,
            start_date=start_date,
            end_date=end_date
        )
        
        # Estrai valori (dovrebbe esserci solo una riga)
        if df.empty:
            raise ValueError(f"Nessun dato trovato per il periodo {start_date} - {end_date}")
        
        row = df.iloc[0]
        
        return PeriodMetrics(
            views=int(float(row.get(MetricType.VIEWS.value, 0))),
            users=int(float(row.get(MetricType.USERS.value, 0))),
            avg_session_duration=float(row.get(MetricType.SESSION_DURATION.value, 0)),
            engagement_rate=float(row.get(MetricType.ENGAGEMENT_RATE.value, 0)),
            start_date=start_date,
            end_date=end_date
        )
    
    def calculate_previous_period(
        self,
        start_date: str,
        end_date: str
    ) -> Tuple[str, str]:
        """
        Calcola le date del periodo precedente con la stessa durata
        
        Args:
            start_date: Data inizio periodo corrente
            end_date: Data fine periodo corrente
            
        Returns:
            Tupla (previous_start, previous_end)
        """
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
        
        # Calcola durata del periodo
        duration = (end - start).days + 1
        
        # Periodo precedente termina il giorno prima dell'inizio del periodo corrente
        previous_end = start - timedelta(days=1)
        previous_start = previous_end - timedelta(days=duration - 1)
        
        return (
            previous_start.strftime("%Y-%m-%d"),
            previous_end.strftime("%Y-%m-%d")
        )
    
    def compare_periods(
        self,
        current_start: str,
        current_end: str,
        previous_start: Optional[str] = None,
        previous_end: Optional[str] = None
    ) -> Dict[str, MetricComparison]:
        """
        Confronta metriche tra periodo corrente e precedente
        
        Args:
            current_start: Inizio periodo corrente
            current_end: Fine periodo corrente
            previous_start: Inizio periodo precedente (calcolato automaticamente se None)
            previous_end: Fine periodo precedente (calcolato automaticamente se None)
            
        Returns:
            Dictionary con confronti per ogni metrica
        """
        # Calcola periodo precedente se non fornito
        if previous_start is None or previous_end is None:
            previous_start, previous_end = self.calculate_previous_period(
                current_start, current_end
            )
        
        # Ottieni metriche per entrambi i periodi
        current = self.get_period_metrics(current_start, current_end)
        previous = self.get_period_metrics(previous_start, previous_end)
        
        # Salva i periodi per riferimento
        self.current_period = current
        self.previous_period = previous
        
        # Crea confronti
        comparisons = {
            'views': MetricComparison(
                current_value=current.views,
                previous_value=previous.views,
                metric_name="visualizzazioni"
            ),
            'users': MetricComparison(
                current_value=current.users,
                previous_value=previous.users,
                metric_name="utenti attivi"
            ),
            'session_duration': MetricComparison(
                current_value=current.avg_session_duration,
                previous_value=previous.avg_session_duration,
                metric_name="minutaggio"
            ),
            'engagement_rate': MetricComparison(
                current_value=current.engagement_rate,
                previous_value=previous.engagement_rate,
                metric_name="tasso medio di engagement"
            )
        }
        
        return comparisons
    
    def format_number(self, num: float) -> str:
        """
        Formatta un numero con suffisso k se > 1000
        
        Args:
            num: Numero da formattare
            
        Returns:
            Stringa formattata (es: "23.1k" o "456")
        """
        if num >= 1000:
            return f"{num / 1000:.1f}k"
        return f"{int(num)}"
    
    def generate_report(
        self,
        current_start: str,
        current_end: str,
        previous_start: Optional[str] = None,
        previous_end: Optional[str] = None,
        period_name: str = "nell'ultima settimana",
        show_unchanged_label: bool = True
    ) -> str:
        """
        Genera report testuale del confronto
        
        Args:
            current_start: Inizio periodo corrente
            current_end: Fine periodo corrente
            previous_start: Inizio periodo precedente (opzionale)
            previous_end: Fine periodo precedente (opzionale)
            period_name: Nome descrittivo del periodo (es: "nell'ultima settimana")
            show_unchanged_label: Se True, mostra "sostanzialmente invariato" per cambi < 1%
                                  Se False, mostra sempre la percentuale esatta
            
        Returns:
            Report formattato come stringa
        """
        # Esegui confronto
        comparisons = self.compare_periods(
            current_start, current_end, previous_start, previous_end
        )
        
        # Genera report
        lines = [
            f"Fotografia delle performance del sito {period_name}:",
            ""
        ]
        
        # Visualizzazioni
        views = comparisons['views']
        lines.append(
            f"visualizzazioni: {self.format_number(views.current_value)} "
            f"({views.format_change(show_unchanged_label)})"
        )
        
        # Utenti
        users = comparisons['users']
        lines.append(
            f"utenti attivi: {self.format_number(users.current_value)} "
            f"({users.format_change(show_unchanged_label)})"
        )
        
        # Minutaggio
        lines.append(
            f"minutaggio: {self.current_period.format_duration()} "
            f"({comparisons['session_duration'].format_change(show_unchanged_label)})"
        )
        
        # Engagement rate
        lines.append(
            f"tasso medio di engagement: {self.current_period.format_engagement_rate()} "
            f"({comparisons['engagement_rate'].format_change(show_unchanged_label)})"
        )
        
        return "\n".join(lines)


def main():
    """Funzione principale con esempi di utilizzo"""
    
    # Inizializza analyzer
    analyzer = GA4PerformanceAnalyzer()
    
    # Esempio 1: Ultima settimana vs settimana precedente
    print("=" * 70)
    print("ESEMPIO 1: Ultima settimana completa")
    print("=" * 70)
    
    # Calcola ultima settimana completa (lunedì-domenica)
    today = date.today()
    days_since_monday = today.weekday()
    last_sunday = today - timedelta(days=days_since_monday + 1)
    last_monday = last_sunday - timedelta(days=6)
    
    report = analyzer.generate_report(
        current_start=last_monday.strftime("%Y-%m-%d"),
        current_end=last_sunday.strftime("%Y-%m-%d"),
        period_name="nell'ultima settimana"
    )
    print(report)
    
    print("\n" + "=" * 70)
    print("ESEMPIO 2: Ultimi 7 giorni")
    print("=" * 70)
    
    # Ultimi 7 giorni
    end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    
    report = analyzer.generate_report(
        current_start=start_date,
        current_end=end_date,
        period_name="negli ultimi 7 giorni"
    )
    print(report)
    
    print("\n" + "=" * 70)
    print("ESEMPIO 3: Periodo personalizzato")
    print("=" * 70)
    
    # Periodo personalizzato: prima settimana di dicembre
    report = analyzer.generate_report(
        current_start="2025-12-01",
        current_end="2025-12-07",
        period_name="nella prima settimana di dicembre"
    )
    print(report)
    
    print("\n" + "=" * 70)
    print("DETTAGLI TECNICI")
    print("=" * 70)
    print(f"Periodo corrente: {analyzer.current_period.start_date} - {analyzer.current_period.end_date}")
    print(f"Periodo precedente: {analyzer.previous_period.start_date} - {analyzer.previous_period.end_date}")


if __name__ == "__main__":
    main()
