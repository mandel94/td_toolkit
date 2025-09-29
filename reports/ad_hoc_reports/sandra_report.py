import sys
import os
sys.path.append(os.path.join(os.path.abspath(__file__), "..", "..", ".."))
sys.path.append(os.path.join(os.path.abspath(__file__), "..", ".."))  # Adjust path as needed
sys.path.append(os.path.join(os.path.abspath(__file__), ".."))  # Adjust path as needed
import pandas as pd
from datetime import datetime, timedelta
from ga4_api.ga4_api import Ga4Client
from etl.page_and_screen_etl import PageAndScreenETLFactory
from map_ga4_categories import map_ga4_categories
from td_data_toolkit.article_analytics.metadata import get_article_metadata
from config import OUTPUT_DIR, WEEKLY_OUTPUT_DIR

# Configurazione
PROPERTY_ID = '394327334'
DIMENSIONS = ['pagePath']
METRICS = ['screenPageViews', 'engagementRate', 'bounceRate', 'averageSessionDuration']  # Aggiunto engagementTime per avere un secondo metrica utile
DAYS = 7
N_TOP = 100
DOMAIN = "https://taxidrivers.it"
# Make EXCEL_OUTPUT dynamic based on DAYS and current date
end_date = datetime.now().date()
start_date = end_date - timedelta(days=DAYS)
OUTPUT_DIR = os.path.join(WEEKLY_OUTPUT_DIR, 'Weekly Midreports')
MIDREPORT_FILENAME = os.path.join(OUTPUT_DIR, f"top_articles_{start_date.strftime('%y%m%d')}_{end_date.strftime('%y%m%d')}.xlsx")

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
    print(f"\n=== TOP {N_TOP} ARTICOLI DELLA SETTIMANA ===\n")
    # Calcola le date
    # Already calculated above for MIDREPORT_FILENAME  
    print(f"Periodo analizzato: {start_date} → {end_date}")

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

    # Estrai i titoli, autori e date
    print(f"Estrazione titoli articoli, autori e date per la top {N_TOP}...")
    titles = []
    authors = []
    pub_dates = []
    for idx, path in enumerate(top_df["pagePath"], 1):
        url = DOMAIN + path if path.startswith("/") else DOMAIN + "/" + path
        print(f"[{idx}/{N_TOP}] Recupero metadati per: {url}")
        pub_date, author, title = get_article_metadata(url)
        titles.append(title if title else "Titolo non trovato")
        authors.append(author if author else "Autore non trovato")
        pub_dates.append(pub_date.strftime('%Y-%m-%d') if pub_date else "Data non trovata")
    top_df["title"] = titles
    top_df["author"] = authors
    top_df["category"] = top_df["pagePath"].apply(map_ga4_categories)
    top_df["publication_date"] = pub_dates
    # Reorder columns as requested
    ordered_cols = [
        'title',
        'pagePath',
        'publication_date',
        'author',
        'category',
        'screenPageViews',
        'engagementRate',
        'bounceRate',
        'averageSessionDuration (s)'
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


