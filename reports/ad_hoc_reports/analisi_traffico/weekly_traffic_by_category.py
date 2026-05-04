"""
weekly_traffic_by_category.py

Recupera le metriche GA4 per tutte le settimane dall'inizio del 2025 ad oggi,
aggrega per pagePath, applica l'ETL di pulizia e mappa le categorie.

Output: weekly_traffic_by_category.csv nella stessa cartella.

--- Estendere le metriche ---
Per aggiungere una metrica GA4, aggiungila alla lista METRICS qui sotto.
L'intera pipeline la gestirà automaticamente.
"""

import sys
import time
from pathlib import Path
from datetime import date, timedelta

import pandas as pd

# --- path setup ---
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from ga4_api.ga4_api import Ga4Client
from etl.page_and_screen_etl import PageAndScreenETLFactory
from reports.map_ga4_categories import map_ga4_categories
from scrape_content.ArticleScraper import ArticleScraper

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PROPERTY_ID = "394327334"

# Aggiungere/rimuovere metriche GA4 qui per estendere la pipeline
METRICS = [
    "sessions",
    "engagedSessions",
    "screenPageViews",
]

DIMENSIONS = ["pagePath"]

START_OF_PERIOD = date(2025, 1, 6)   # primo lunedì del 2025
END_OF_PERIOD   = date.today()

OUTPUT_FILE = Path(__file__).parent / "weekly_traffic_by_category.csv"

# ---------------------------------------------------------------------------
# Scraping configuration
# ---------------------------------------------------------------------------

DOMAIN = "https://taxidrivers.it"

# Secondi di attesa tra una richiesta e l'altra (cortesia verso il server)
SCRAPE_DELAY_BETWEEN_ARTICLES = 1   # secondi

# Dimensione di ciascun chunk di URL da scrapare in sequenza
SCRAPE_CHUNK_SIZE = 20

# Pausa aggiuntiva tra un chunk e il successivo
SCRAPE_DELAY_BETWEEN_CHUNKS = 60    # secondi (1 minuto)

# Cache su file JSON: pagePath -> publication_date
SCRAPE_CACHE_FILE = Path(__file__).parent / "scrape_cache.json"


def load_scrape_cache() -> dict:
    """Carica la cache da disco. Restituisce un dizionario {pagePath: publication_date}."""
    if SCRAPE_CACHE_FILE.exists():
        import json
        with open(SCRAPE_CACHE_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_scrape_cache(cache: dict) -> None:
    """Salva la cache su disco."""
    import json
    with open(SCRAPE_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def generate_weeks(start: date, end: date) -> list[tuple[str, date, date]]:
    """
    Genera tutte le settimane lunedì–domenica dall'inizio del periodo a oggi.

    Restituisce una lista di (nome_settimana, data_inizio, data_fine).
    Il nome ha formato "YYMM{week}" dove week è il numero ordinale della
    settimana nel mese (1–4), es. "250101" per la prima settimana di gen 2025.
    """
    weeks = []
    current = start - timedelta(days=start.weekday())  # normalizza a lunedì

    while current <= end:
        week_start = current
        week_end   = current + timedelta(days=6)
        if week_end > end:
            week_end = end

        week_of_month = min((week_start.day - 1) // 7 + 1, 4)
        name = f"{week_start.strftime('%y%m')}{week_of_month}"
        weeks.append((name, week_start, week_end))

        current += timedelta(weeks=1)

    return weeks


def fetch_week(ga4: Ga4Client, week_start: date, week_end: date) -> pd.DataFrame:
    """Interroga GA4 per la settimana indicata e restituisce il DataFrame grezzo."""
    return ga4.run_query(
        property_id=PROPERTY_ID,
        dimensions=DIMENSIONS,
        metrics=METRICS,
        start_date=week_start.strftime("%Y-%m-%d"),
        end_date=week_end.strftime("%Y-%m-%d"),
    )


def apply_etl(df: pd.DataFrame) -> pd.DataFrame:
    """Applica il PageAndScreenETL (rimuove non-.html e homepage)."""
    etl = PageAndScreenETLFactory.get_etl("en", df=df)
    etl.apply_transformations()
    return etl.df


def enrich(df: pd.DataFrame, week_name: str) -> pd.DataFrame:
    """Aggiunge le colonne week_name e category al DataFrame."""
    df = df.copy()
    df.insert(0, "week_name", week_name)
    df["category"] = df["pagePath"].apply(map_ga4_categories)
    return df


def scrape_publication_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Arricchisce il DataFrame con la data di pubblicazione di ogni articolo
    recuperata via scraping da taxidrivers.it.

    Usa una cache su file JSON (scrape_cache.json) per evitare di ri-scrapare
    URL già visitati. Solo i path non in cache vengono richiesti al server.
    La cache viene aggiornata su disco dopo ogni chunk.
    """
    scraper = ArticleScraper(
        domain=DOMAIN,
        features=["publication_date"],
        delay=SCRAPE_DELAY_BETWEEN_ARTICLES,
    )

    cache = load_scrape_cache()

    unique_paths = df["pagePath"].unique().tolist()
    paths_to_fetch = [p for p in unique_paths if p not in cache]
    cache_hits = len(unique_paths) - len(paths_to_fetch)

    print(f"\nScraping publication_date: {len(unique_paths)} URL unici "
          f"→ {cache_hits} da cache, {len(paths_to_fetch)} da scrapare "
          f"(chunk={SCRAPE_CHUNK_SIZE}, delay={SCRAPE_DELAY_BETWEEN_ARTICLES}s, "
          f"inter-chunk={SCRAPE_DELAY_BETWEEN_CHUNKS}s)")

    chunks = [
        paths_to_fetch[i : i + SCRAPE_CHUNK_SIZE]
        for i in range(0, len(paths_to_fetch), SCRAPE_CHUNK_SIZE)
    ]

    for chunk_idx, chunk in enumerate(chunks, start=1):
        print(f"  Chunk {chunk_idx}/{len(chunks)} ({len(chunk)} URL)")
        for path in chunk:
            result = scraper.scrape_article(path)
            cache[path] = result.get("publication_date")

        # Salva la cache dopo ogni chunk per non perdere il lavoro fatto
        save_scrape_cache(cache)

        if chunk_idx < len(chunks):
            print(f"  Pausa inter-chunk ({SCRAPE_DELAY_BETWEEN_CHUNKS}s)...")
            time.sleep(SCRAPE_DELAY_BETWEEN_CHUNKS)

    # Costruisce il DataFrame dei risultati attingendo alla cache (include cache hits)
    pub_dates_df = pd.DataFrame([
        {"pagePath": p, "publication_date": cache.get(p)}
        for p in unique_paths
    ])
    enriched = df.merge(pub_dates_df, on="pagePath", how="left")
    return enriched


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ga4   = Ga4Client()
    weeks = generate_weeks(START_OF_PERIOD, END_OF_PERIOD)

    print(f"Settimane da elaborare: {len(weeks)}")
    print(f"Periodo: {weeks[0][1]} → {weeks[-1][2]}\n")

    frames: list[pd.DataFrame] = []

    for week_name, week_start, week_end in weeks:
        print(f"Fetching {week_name} ...", end=" ", flush=True)
        try:
            raw = fetch_week(ga4, week_start, week_end)
            if raw.empty:
                print("nessun dato, skip.")
                continue
            cleaned  = apply_etl(raw)
            enriched = enrich(cleaned, week_name)
            frames.append(enriched)
            print(f"OK ({len(enriched)} righe)")
        except Exception as exc:
            print(f"ERRORE: {exc}")

    if not frames:
        print("Nessun dato recuperato. Output non generato.")
        return

    result = pd.concat(frames, ignore_index=True)

    # cast metriche a numerico (GA4 le restituisce come stringhe)
    for col in METRICS:
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors="coerce")

    result.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print(f"\nOutput salvato in: {OUTPUT_FILE}")
    print(f"Totale righe: {len(result)} | Settimane: {result['week_name'].nunique()}")


if __name__ == "__main__":
    main()
