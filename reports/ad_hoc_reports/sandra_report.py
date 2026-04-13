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
from report_config import OUTPUT_DIR, WEEKLY_OUTPUT_DIR
import time

# Project root directory (2 levels up from this file)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Configurazione
PROPERTY_ID = '394327334'
DIMENSIONS = ['pagePath']
METRICS = ['screenPageViews', 'engagementRate', 'averageSessionDuration']  # bounceRate rimosso (ridondante con engagementRate)
DEFAULT_DAYS = 7
N_TOP = 100
DOMAIN = "https://taxidrivers.it"
MOJIBAKE_MARKERS = ("Ã", "â€™", "â€œ", "â€", "Â")


def _fix_mojibake_text(value):
    if not isinstance(value, str) or not value:
        return value
    text = value.strip()
    if not any(marker in text for marker in MOJIBAKE_MARKERS):
        return text

    candidates = [text]
    for source_encoding in ("latin-1", "cp1252"):
        try:
            repaired = text.encode(source_encoding).decode("utf-8")
            if repaired:
                candidates.append(repaired.strip())
        except (UnicodeEncodeError, UnicodeDecodeError):
            continue

    def _mojibake_score(s):
        return sum(s.count(marker) for marker in MOJIBAKE_MARKERS)

    best = min(candidates, key=_mojibake_score)
    return best


def _normalize_text_columns(df: pd.DataFrame, columns=None) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    cols = columns
    if cols is None:
        cols = df.select_dtypes(include=["object"]).columns.tolist()

    for col in cols:
        if col in df.columns:
            df[col] = df[col].apply(_fix_mojibake_text)
    return df


def _is_timeout_error(error) -> bool:
    message = str(error).lower()
    return "timeout" in message or "timed out" in message


def get_article_metadata_with_retry(url, max_retries=5, retry_delay=10):
    attempt = 0
    while attempt <= max_retries:
        attempt += 1
        try:
            return get_article_metadata(url)
        except Exception as error:
            if not _is_timeout_error(error) or attempt > max_retries:
                raise
            backoff = retry_delay * attempt
            print(
                f"Timeout for {url}, retrying in {backoff}s "
                f"({attempt}/{max_retries})..."
            )
            time.sleep(backoff)
    return None, None, None

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

BUCKET_LABELS = ["Basso", "Medio-basso", "Nella media", "Medio-alto", "Alto"]


def assign_performance_buckets(
    df: pd.DataFrame,
    metrics: list[str],
    labels: list[str] = None,
    suffix: str = "_fascia"
) -> pd.DataFrame:
    """
    Assign each article to one of 5 performance buckets for each metric,
    based on quintiles (0-20%, 20-40%, 40-60%, 60-80%, 80-100%).

    Bucket labels (low → high):
        Basso | Medio-basso | Nella media | Medio-alto | Alto
    """
    used_labels = labels if labels is not None else BUCKET_LABELS
    for metric in metrics:
        if metric not in df.columns:
            continue
        col = pd.to_numeric(df[metric], errors="coerce")
        bucket_col = metric + suffix
        try:
            df[bucket_col] = pd.qcut(
                col,
                q=5,
                labels=used_labels,
                duplicates="drop"
            ).astype(str)
        except ValueError:
            # Fallback: manual percentile cut when qcut can't form 5 unique bins
            p = [col.quantile(q) for q in (0.0, 0.20, 0.40, 0.60, 0.80, 1.0)]
            # Ensure strictly increasing breakpoints
            breakpoints = sorted(set(p))
            if len(breakpoints) < 2:
                df[bucket_col] = used_labels[2]  # all values → "Nella media"
            else:
                n_bins = len(breakpoints) - 1
                adjusted_labels = used_labels[:n_bins] if n_bins <= len(used_labels) else used_labels
                df[bucket_col] = pd.cut(
                    col,
                    bins=breakpoints,
                    labels=adjusted_labels,
                    include_lowest=True
                ).astype(str)

        # Print median and bucket distribution for transparency
        median_val = col.median()
        print(f"  {metric}: mediana={median_val:.2f}")
        dist = df[bucket_col].value_counts().reindex(used_labels, fill_value=0)
        for label, count in dist.items():
            print(f"    {label}: {count}")
    return df


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
    failed_items = []
    for idx, path in enumerate(top_df["pagePath"], 1):
        url = DOMAIN + path if path.startswith("/") else DOMAIN + "/" + path
        print(f"[{idx}/{N_TOP}] Recupero metadati per: {url}")
        try:
            pub_date, author, title = get_article_metadata_with_retry(url)
            pub_date_str = pub_date.strftime('%Y-%m-%d') if pub_date else "None"
            title_str = str(title).replace("\n", " ").strip() if title else "None"
            if len(title_str) > 80:
                title_str = title_str[:80] + "..."
            print(
                f"[{idx}/{N_TOP}] Scraped output | "
                f"title={title_str} | "
                f"author={author if author else 'None'} | "
                f"pub_date={pub_date_str}"
            )
            if not any((pub_date, author, title)):
                failed_items.append((idx - 1, url))
            titles.append(title if title else "Titolo non trovato")
            authors.append(author if author else "Autore non trovato")
            pub_dates.append(pub_date.strftime('%Y-%m-%d') if pub_date else "Data non trovata")
        except Exception as e:
            print(f"  ⚠ Errore: {str(e)[:50]}")
            titles.append("Errore recupero titolo")
            authors.append("Errore recupero autore")
            pub_dates.append("Errore recupero data")
            failed_items.append((idx - 1, url))

    if failed_items:
        print(f"Final retry pass started for {len(failed_items)} failed URLs...")
        still_failed = 0
        for retry_pos, (item_idx, url) in enumerate(failed_items, 1):
            try:
                pub_date, author, title = get_article_metadata_with_retry(url)
                title_str = str(title).replace("\n", " ").strip() if title else "None"
                if len(title_str) > 80:
                    title_str = title_str[:80] + "..."
                pub_date_str = pub_date.strftime('%Y-%m-%d') if pub_date else "None"
                print(
                    f"[Retry {retry_pos}/{len(failed_items)}] Scraped output | "
                    f"title={title_str} | "
                    f"author={author if author else 'None'} | "
                    f"pub_date={pub_date_str}"
                )
                if any((pub_date, author, title)):
                    titles[item_idx] = title if title else "Titolo non trovato"
                    authors[item_idx] = author if author else "Autore non trovato"
                    pub_dates[item_idx] = pub_date.strftime('%Y-%m-%d') if pub_date else "Data non trovata"
                else:
                    still_failed += 1
            except Exception as e:
                print(f"Final retry error for {url}: {str(e)[:80]}")
                still_failed += 1

        print(
            f"Final retry pass completed: recovered={len(failed_items) - still_failed} "
            f"still_failed={still_failed}"
        )

    top_df["title"] = titles
    top_df["author"] = authors
    top_df["category"] = top_df["pagePath"].apply(map_ga4_categories)
    top_df["publication_date"] = pub_dates
    top_df = _normalize_text_columns(top_df)
    
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
    
    # Assign performance buckets for key metrics
    print("\n📊 Assegnazione fasce di performance...")
    top_df = assign_performance_buckets(
        top_df,
        metrics=['screenPageViews', 'engagementRate', 'averageSessionDuration']
    )
    print("✓ Fasce assegnate per: screenPageViews, engagementRate, averageSessionDuration")

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
        'anomaly_flag',
        'screenPageViews_fascia',
        'engagementRate_fascia',
        'averageSessionDuration_fascia'
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
    
    # Salva in formato CSV per notebook EDA (sempre nello stesso path)
    csv_output_dir = os.path.join(PROJECT_ROOT, 'output')
    os.makedirs(csv_output_dir, exist_ok=True)
    csv_filename = os.path.join(csv_output_dir, 'sandra_midreport_data.csv')
    top_df.to_csv(csv_filename, index=False)
    print(f"Dati salvati anche in {csv_filename} per analisi EDA\n")
    
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


