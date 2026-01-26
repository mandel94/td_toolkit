"""
Editorial Summary Generator
Genera sintesi editoriali narrative basate sui dati degli articoli più performanti.
"""
import sys
import os
from typing import Dict, List, Optional
from dataclasses import dataclass
import pandas as pd

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


@dataclass
class Article:
    """Rappresenta un articolo con le sue metriche"""
    title: str
    page_path: str
    publication_date: str
    author: str
    category: str
    views: int
    engagement_rate: float
    bounce_rate: float
    avg_session_duration: float
    
    def format_engagement(self) -> str:
        """Formatta engagement rate come percentuale"""
        return f"{self.engagement_rate * 100:.0f}%"
    
    def format_bounce(self) -> str:
        """Formatta bounce rate come percentuale"""
        return f"{self.bounce_rate * 100:.0f}%"
    
    def format_duration(self) -> str:
        """Formatta durata in formato mm:ss"""
        minutes = int(self.avg_session_duration // 60)
        seconds = int(self.avg_session_duration % 60)
        return f"{minutes}m {seconds:02d}s"


class EditorialSummaryGenerator:
    """Genera sintesi editoriali basate sui dati degli articoli più performanti"""
    
    def __init__(self):
        """Inizializza il generatore"""
        self.articles: List[Article] = []
        self.category_labels = {
            "Si farà": "Gli articoli più attesi hanno avuto molto successo",
            "Anticipazioni": "Tra le anticipazioni, hanno funzionato bene",
            "Recensioni": "Per quanto riguarda le recensioni",
            "Recensioni / In Sala": "Tra i film in sala",
            "Interviste": "Per quanto riguarda le interviste, hanno avuto molto successo",
            "News": "Le news più viste sono state"
        }
    
    def load_from_excel(self, file_path: str) -> None:
        """
        Carica dati da file Excel
        
        Args:
            file_path: Percorso del file Excel
        """
        df = pd.read_excel(file_path)
        
        # Converti i dati in oggetti Article
        self.articles = []
        for _, row in df.iterrows():
            article = Article(
                title=str(row.get('title', 'Senza titolo')),
                page_path=str(row.get('pagePath', '')),
                publication_date=str(row.get('publication_date', '')),
                author=str(row.get('author', 'Redazione')),
                category=str(row.get('category', 'Non categorizzato')),
                views=int(row.get('screenPageViews', 0)),
                engagement_rate=float(row.get('engagementRate', 0.0)),
                bounce_rate=float(row.get('bounceRate', 0.0)),
                avg_session_duration=float(row.get('averageSessionDuration', 0.0))
            )
            self.articles.append(article)
    
    def get_top_articles(
        self,
        n: int = 10,
        sort_by: str = 'views'
    ) -> List[Article]:
        """
        Ottieni i top N articoli per una metrica specifica
        
        Args:
            n: Numero di articoli da restituire
            sort_by: Metrica per ordinamento ('views', 'engagement_rate', 'avg_session_duration')
            
        Returns:
            Lista di articoli ordinati
        """
        if sort_by == 'views':
            sorted_articles = sorted(self.articles, key=lambda x: x.views, reverse=True)
        elif sort_by == 'engagement_rate':
            sorted_articles = sorted(self.articles, key=lambda x: x.engagement_rate, reverse=True)
        elif sort_by == 'avg_session_duration':
            sorted_articles = sorted(self.articles, key=lambda x: x.avg_session_duration, reverse=True)
        else:
            raise ValueError(f"Metrica non valida: {sort_by}")
        
        return sorted_articles[:n]
    
    def group_by_category(self) -> Dict[str, List[Article]]:
        """
        Raggruppa articoli per categoria
        
        Returns:
            Dictionary con categoria come chiave e lista di articoli come valore
        """
        groups = {}
        for article in self.articles:
            if article.category not in groups:
                groups[article.category] = []
            groups[article.category].append(article)
        
        # Ordina articoli in ogni categoria per views
        for category in groups:
            groups[category] = sorted(
                groups[category],
                key=lambda x: x.views,
                reverse=True
            )
        
        return groups
    
    def _get_metric_note(self, article: Article) -> str:
        """
        Genera nota descrittiva basata sulle metriche
        
        Args:
            article: Articolo da analizzare
            
        Returns:
            Nota descrittiva o stringa vuota
        """
        notes = []
        
        # Alta engagement
        if article.engagement_rate > 0.6:
            notes.append("con grande partecipazione dei lettori")
        
        # Bassa bounce rate (lettori rimangono sulla pagina)
        if article.bounce_rate < 0.35:
            notes.append("che tiene i lettori sulla pagina")
        
        # Alta durata sessione
        if article.avg_session_duration > 120:
            notes.append("letta più a lungo delle altre")
        
        if notes:
            return ", " + " e ".join(notes)
        return ""
    
    def _find_most_read_overall(self, articles: List[Article]) -> Optional[Article]:
        """Trova l'articolo più letto in assoluto"""
        if not articles:
            return None
        return max(articles, key=lambda x: x.views)
    
    def _find_longest_read(self, articles: List[Article]) -> Optional[Article]:
        """Trova l'articolo letto più a lungo"""
        if not articles:
            return None
        return max(articles, key=lambda x: x.avg_session_duration)
    
    def generate_editorial_summary(
        self,
        min_views: int = 100,
        max_articles_per_category: int = 4,
        categories_order: Optional[List[str]] = None
    ) -> str:
        """
        Genera sintesi editoriale in stile narrativo
        
        Args:
            min_views: Visualizzazioni minime per includere un articolo
            max_articles_per_category: Numero massimo di articoli per categoria
            categories_order: Ordine preferito delle categorie (opzionale)
            
        Returns:
            Sintesi editoriale formattata
        """
        # Filtra articoli con visualizzazioni minime
        relevant_articles = [a for a in self.articles if a.views >= min_views]
        
        if not relevant_articles:
            return "Nessun articolo trovato con i criteri specificati."
        
        # Raggruppa per categoria
        by_category = {}
        for article in relevant_articles:
            if article.category not in by_category:
                by_category[article.category] = []
            by_category[article.category].append(article)
        
        # Ordina articoli in ogni categoria per views
        for category in by_category:
            by_category[category] = sorted(
                by_category[category],
                key=lambda x: x.views,
                reverse=True
            )
        
        # Determina ordine categorie
        if categories_order:
            # Usa ordine specificato
            ordered_categories = [c for c in categories_order if c in by_category]
            # Aggiungi categorie rimanenti ordinate per total views
            remaining = {k: v for k, v in by_category.items() if k not in categories_order}
            ordered_categories.extend(
                sorted(remaining.keys(), key=lambda x: sum(a.views for a in by_category[x]), reverse=True)
            )
        else:
            # Ordina per total views nella categoria
            ordered_categories = sorted(
                by_category.keys(),
                key=lambda x: sum(a.views for a in by_category[x]),
                reverse=True
            )
        
        # Genera sintesi
        lines = []
        
        for category in ordered_categories:
            articles = by_category[category][:max_articles_per_category]
            
            if not articles:
                continue
            
            # Intestazione categoria
            category_label = self.category_labels.get(category, f"Tra i contenuti di {category}")
            lines.append(f"{category_label}:\n")
            
            # Articolo principale (più letto)
            top_article = articles[0]
            most_read_in_week = self._is_most_read_in_week(top_article, relevant_articles)
            
            if most_read_in_week:
                lines.append(f"{top_article.title} è stato il più letto della settimana")
            else:
                metric_note = self._get_metric_note(top_article)
                lines.append(f"{top_article.title}{metric_note}")
            
            # Altri articoli della categoria
            for i, article in enumerate(articles[1:], start=1):
                metric_note = self._get_metric_note(article)
                
                # Varia il connettivo
                if i == 1:
                    prefix = ""
                elif i == len(articles) - 1:
                    prefix = "Molto interesse anche per "
                else:
                    prefix = "Bene anche "
                
                lines.append(f"{prefix}{article.title}{metric_note}")
            
            lines.append("")  # Riga vuota tra categorie
        
        return "\n".join(lines)
    
    def _is_most_read_in_week(self, article: Article, all_articles: List[Article]) -> bool:
        """Verifica se l'articolo è il più letto in assoluto"""
        max_views = max(a.views for a in all_articles)
        return article.views == max_views
    
    def get_stats(self) -> Dict[str, any]:
        """
        Ottieni statistiche sui dati caricati
        
        Returns:
            Dictionary con statistiche
        """
        if not self.articles:
            return {
                'total_articles': 0,
                'total_views': 0,
                'avg_engagement': 0,
                'categories': []
            }
        
        return {
            'total_articles': len(self.articles),
            'total_views': sum(a.views for a in self.articles),
            'avg_engagement': sum(a.engagement_rate for a in self.articles) / len(self.articles),
            'avg_duration': sum(a.avg_session_duration for a in self.articles) / len(self.articles),
            'categories': list(set(a.category for a in self.articles)),
            'top_article': max(self.articles, key=lambda x: x.views).title if self.articles else None
        }


def main():
    """Funzione principale con esempio di utilizzo"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Genera sintesi editoriale da file Excel')
    parser.add_argument('file', nargs='?', default='top_100_articles_week 50.xlsx', 
                        help='Percorso del file Excel (default: top_100_articles_week 50.xlsx)')
    parser.add_argument('--min-views', type=int, default=100, help='Visualizzazioni minime')
    parser.add_argument('--max-per-category', type=int, default=4, help='Articoli max per categoria')
    parser.add_argument('--stats', action='store_true', help='Mostra solo statistiche')
    
    args = parser.parse_args()
    
    # Inizializza generatore
    generator = EditorialSummaryGenerator()
    
    # Carica dati
    print(f"Caricamento dati da {args.file}...")
    generator.load_from_excel(args.file)
    
    if args.stats:
        # Mostra statistiche
        stats = generator.get_stats()
        print("\n" + "=" * 70)
        print("STATISTICHE")
        print("=" * 70)
        print(f"Articoli totali: {stats['total_articles']}")
        print(f"Visualizzazioni totali: {stats['total_views']:,}")
        print(f"Engagement medio: {stats['avg_engagement']*100:.1f}%")
        print(f"Durata media: {stats['avg_duration']:.0f}s")
        print(f"Categorie: {', '.join(stats['categories'])}")
        print(f"Articolo più letto: {stats['top_article']}")
    else:
        # Genera sintesi
        print("\n" + "=" * 70)
        print("SINTESI EDITORIALE")
        print("=" * 70)
        summary = generator.generate_editorial_summary(
            min_views=args.min_views,
            max_articles_per_category=args.max_per_category
        )
        print(summary)


if __name__ == "__main__":
    main()
