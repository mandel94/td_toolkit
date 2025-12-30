import sys
import os
import argparse
sys.path.append(os.path.join(os.path.abspath(__file__), "..", "..", ".."))
sys.path.append(os.path.join(os.path.abspath(__file__), "..", ".."))  # Adjust path as needed
sys.path.append(os.path.join(os.path.abspath(__file__), ".."))  # Adjust path as needed
import pandas as pd
from datetime import datetime, timedelta
from ga4_api.ga4_api import Ga4Client
from etl.page_and_screen_etl import PageAndScreenETLFactory
from etl.content_scoring import (
    ContentScoreCalculator,
    ContentScoreSegmentation,
    ContentScoreValidator,
    ContentScoringConfig
)
from map_ga4_categories import map_ga4_categories
from td_data_toolkit.article_analytics.metadata import get_article_metadata
from config import OUTPUT_DIR, WEEKLY_OUTPUT_DIR

# Configurazione
PROPERTY_ID = '394327334'
DIMENSIONS = ['pagePath']
METRICS = ['screenPageViews', 'engagementRate', 'averageSessionDuration']  # bounceRate rimosso (ridondante con engagementRate)
DEFAULT_DAYS = 7
N_TOP = 100
DOMAIN = "https://taxidrivers.it"

def parse_arguments():
    """Parse command line arguments for the sandra report."""
    parser = argparse.ArgumentParser(description='Generate Sandra report for top articles')
    parser.add_argument('--days', type=int, default=DEFAULT_DAYS,
                        help=f'Number of days to analyze starting from yesterday (default: {DEFAULT_DAYS})')
    parser.add_argument('--start-date', type=str,
                        help='Start date in YYYY-MM-DD format (overrides --days)')
    parser.add_argument('--end-date', type=str,
                        help='End date in YYYY-MM-DD format (overrides --days)')
    return parser.parse_args()

def calculate_dates(args):
    """Calculate start and end dates based on arguments."""
    if args.start_date and args.end_date:
        # Use explicit start and end dates
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
    elif args.start_date:
        # Use start date with days parameter
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        end_date = start_date + timedelta(days=args.days - 1)
    else:
        # Default behavior: X days starting from yesterday
        yesterday = datetime.now().date() - timedelta(days=1)
        end_date = yesterday
        start_date = end_date - timedelta(days=args.days - 1)
    
    return start_date, end_date

CATEGORIES = {
    "News": {"latest-news", "focus-italia"},
    # "Anticipazioni": {"anticipazioni"},
    "Recensioni": {"review", "netflix-film", "sky-film", "disney-film", "mubi", "mubi-film",
                    "approfondimenti", "streaming"},
    "In Sala": {"in-sala"},
    "Cult Movies": {"cult-movie"},
    "Animazione": {"animazione", "animazione/anime"},
    "Approfondimento": {"approfondimento"},
    "Festival di Cinema": {"festival-di-cinema"},
    "Trailers": {"trailers"},
    "Serie TV": {"serie-tv", "netflix-serie-tv", "prime-video-serietv", "sky-serie-tv",
                 "disney-serietv", "paramount-serie-tv", "appletv-serietv", "tim-vision-serie-tv"},
    "Guide e Film da Vedere": {"film-da-vedere"},
    "Speciali e Magazine": {"magazine-2", "taxidrivers-magazine"},
    "Live Streaming On Demand": {"live-streaming-on-demand"},
    "Rubriche": {"rubriche"},
    "Interviste": {"interviews"}
}

def main():
    # Parse command line arguments
    args = parse_arguments()
    
    # Calculate dates based on arguments
    start_date, end_date = calculate_dates(args)
    
    # Set up output directory and filename
    OUTPUT_DIR = os.path.join(WEEKLY_OUTPUT_DIR, 'Weekly Midreports')
    MIDREPORT_FILENAME = os.path.join(OUTPUT_DIR, f"top_articles_{start_date.strftime('%y%m%d')}_{end_date.strftime('%y%m%d')}.xlsx")
    
    print(f"\n=== TOP {N_TOP} ARTICOLI ===\n")
    print(f"Periodo analizzato: {start_date} → {end_date} ({args.days} giorni)")
    print(f"Output file: {MIDREPORT_FILENAME}\n")

    # Inizializza il client GA4
    print("Inizializzazione client GA4...")
    ga4 = Ga4Client()
    print("Recupero dati da Google Analytics...")
    df = ga4.run_query(
        property_id=PROPERTY_ID,
        dimensions=DIMENSIONS,
        metrics=METRICS,
        start_date=str(start_date),
        end_date=str(end_date)
    )
    print(f"Dati recuperati: {len(df)} righe")

    # Pulisci i dati usando il modulo OOP ETL
    print("Pulizia dati: rimozione homepage e path non .html...")
    etl = PageAndScreenETLFactory.get_etl('en', df=df)
    etl.apply_transformations()
    df = etl.df
    print(f"Dati dopo pulizia: {len(df)} righe")

    # Ordina per visualizzazioni e prendi la top N
    print(f"Ordinamento per '{METRICS[0]}' e selezione top {N_TOP}...")
    df[METRICS[0]] = pd.to_numeric(df[METRICS[0]], errors="coerce").fillna(0)
    df = df.sort_values(METRICS[0], ascending=False)
    top_df = df.head(N_TOP).copy()

    # Estrai i titoli, autori e date con error handling
    print(f"Estrazione titoli articoli, autori e date per la top {N_TOP}...")
    titles = []
    authors = []
    pub_dates = []
    for idx, path in enumerate(top_df["pagePath"], 1):
        url = DOMAIN + path if path.startswith("/") else DOMAIN + "/" + path
        print(f"[{idx}/{N_TOP}] Recupero metadati per: {url}")
        try:
            pub_date, author, title = get_article_metadata(url)
            titles.append(title if title else "Titolo non trovato")
            authors.append(author if author else "Autore non trovato")
            pub_dates.append(pub_date.strftime('%Y-%m-%d') if pub_date else "Data non trovata")
        except Exception as e:
            print(f"  ⚠ Errore: {str(e)[:50]}")
            titles.append("Errore recupero titolo")
            authors.append("Errore recupero autore")
            pub_dates.append("Errore recupero data")
    top_df["title"] = titles
    top_df["author"] = authors
    top_df["category"] = top_df["pagePath"].apply(map_ga4_categories)
    top_df["publication_date"] = pub_dates
    
    # Ensure numeric columns are properly typed before scoring
    print("\nConversione colonne numeriche...")
    numeric_cols = ['screenPageViews', 'engagementRate', 'averageSessionDuration']
    for col in numeric_cols:
        if col in top_df.columns:
            top_df[col] = pd.to_numeric(top_df[col], errors='coerce')
    print(f"✓ Colonne numeriche convertite: {numeric_cols}")
    
    # Calculate Content Scores
    print("\n📊 Calcolo Editorial Score...")
    try:
        # Initialize content scoring with Balanced strategy (Strategy Pattern)
        config = ContentScoringConfig(strategy_name='balanced')
        calculator = ContentScoreCalculator(config)
        segmenter = ContentScoreSegmentation(config)
        validator = ContentScoreValidator(config)
        
        print(f"Strategia utilizzata: {calculator.strategy.get_name()}")
        strategy_weights = calculator.strategy.get_weights()
        print(f"Pesi: Reach={strategy_weights['reach']:.0%}, "
              f"Engagement={strategy_weights['engagement']:.0%}, "
              f"Depth={strategy_weights['depth']:.0%}")
        
        # Calculate scores
        top_df = calculator.calculate(top_df)
        print(f"✓ Editorial Score calcolato per {len(top_df)} articoli")
        
        # Apply segmentation
        top_df = segmenter.segment(top_df)
        print(f"✓ Segmentazione completata")
        
        # Validate and flag anomalies
        is_valid, issues = validator.validate(top_df)
        if not is_valid:
            print(f"⚠ Rilevati {len(issues)} problemi di validazione")
            for issue in issues[:3]:  # Show first 3 issues
                print(f"  - {issue['type']}: {issue['message']}")
        
        top_df = validator.flag_anomalies(top_df)
        
        # Print segment distribution
        segment_dist = top_df['content_segment'].value_counts()
        print(f"\n📋 Distribuzione Segmenti:")
        for segment, count in segment_dist.items():
            print(f"  - {segment}: {count} articoli")
        
        # Print score statistics
        print(f"\n📈 Statistiche Score:")
        print(f"  Media: {top_df['editorial_score'].mean():.2f}")
        print(f"  Mediana: {top_df['editorial_score'].median():.2f}")
        print(f"  Min: {top_df['editorial_score'].min():.2f}")
        print(f"  Max: {top_df['editorial_score'].max():.2f}")
        
    except Exception as e:
        print(f"⚠ Errore nel calcolo Editorial Score: {e}")
        print("Continuazione senza score...")
    
    # Reorder columns as requested
    ordered_cols = [
        'editorial_rank',
        'editorial_score',
        'title',
        'pagePath',
        'publication_date',
        'author',
        'category',
        'content_segment',
        'screenPageViews',
        'engagementRate',
        'averageSessionDuration',
        'feature_reach_rank',
        'feature_engagement_rank',
        'feature_depth_rank',
        'anomaly_flag'
    ]
    # Add any extra columns at the end
    extra_cols = [col for col in top_df.columns if col not in ordered_cols]
    top_df = top_df[[col for col in ordered_cols if col in top_df.columns] + extra_cols]

    # Salva su Excel in output directory
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    print(f"Salvataggio risultati in '{MIDREPORT_FILENAME}'...")
    top_df.to_excel(MIDREPORT_FILENAME, index=False)
    print(f"Top {N_TOP} articoli salvati in {MIDREPORT_FILENAME}\n")
    
    # Auto-open Excel if environment variable is set
    if os.getenv('EXCEL_AUTO_OPEN', 'true').lower() == 'true':
        try:
            os.startfile(MIDREPORT_FILENAME)
            print("File Excel aperto automaticamente.")
        except Exception as e:
            print(f"Impossibile aprire automaticamente il file Excel: {e}")

    # Stampa a schermo in modo leggibile
    print(f"\n📊 TOP {N_TOP} ARTICOLI DELLA SETTIMANA\n")
    for i, row in top_df.iterrows():
        print(f"{i+1}. {row['title']}\n   Path: {row['pagePath']}\n   Visualizzazioni: {int(row[METRICS[0]])}\n")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Errore durante l'esecuzione: {e}", file=sys.stderr)
        sys.exit(1)


