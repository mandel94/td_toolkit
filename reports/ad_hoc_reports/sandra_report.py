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
import requests
from bs4 import BeautifulSoup
from td_data_toolkit.article_analytics.metadata import get_article_metadata

# Configurazione
PROPERTY_ID = '394327334'
DIMENSIONS = ['pagePath']
METRICS = ['screenPageViews', 'engagementRate', 'bounceRate', 'averageSessionDuration']  # Aggiunto engagementTime per avere un secondo metrica utile
DAYS = 7
N_TOP = 100
DOMAIN = "https://taxidrivers.it"
EXCEL_OUTPUT = f"top_articles_last_week_v2.xlsx"


def main():
    print(f"\n=== TOP {N_TOP} ARTICOLI DELLA SETTIMANA ===\n")
    # Calcola le date
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=DAYS)
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
    # Reorder columns so that title is at the beginning
    top_df = top_df[['title', 'pagePath', 'publication_date', 'author', 'category'] + [col for col in top_df.columns if col not in ['title', 'pagePath', 'publication_date', 'author', 'category']]]

    # Salva su Excel
    print(f"Salvataggio risultati in '{EXCEL_OUTPUT}'...")
    top_df.to_excel(EXCEL_OUTPUT, index=False)
    print(f"Top {N_TOP} articoli salvati in {EXCEL_OUTPUT}\n")

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


