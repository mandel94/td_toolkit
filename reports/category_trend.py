"""
category_trend.py
-----------------
Scarica e analizza l'andamento mensile di una categoria editoriale
negli ultimi 6 mesi usando la stessa pipeline del weekly report.

Uso:
    python reports/category_trend.py
    python reports/category_trend.py --category "Si farà"
    python reports/category_trend.py --category "Live Streaming On Demand" --months 6 --top-n 10

Output: JSON in output/ad_hoc/category_trend_<categoria>_<data>.json
"""

import argparse
import json
import os
import sys
from datetime import datetime, date
from calendar import monthrange

import pandas as pd

# ---------------------------------------------------------------------------
# Path setup (stesso schema di weekly_report.py)
# ---------------------------------------------------------------------------
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, ".."))

for _p in (_SCRIPT_DIR, _PROJECT_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ---------------------------------------------------------------------------
# Costanti
# ---------------------------------------------------------------------------
PROPERTY_ID = "394327334"
DOMAIN = "https://taxidrivers.it"
DEFAULT_CATEGORY = "Live Streaming On Demand"
DEFAULT_MONTHS = 6
DEFAULT_TOP_N = 10
MIN_VIEWS = 5  # soglia minima di page views per includere un articolo

METRICS = [
    "screenPageViews",
    "activeUsers",
    "engagedSessions",
    "sessions",
    "averageSessionDuration",
]

VALID_CATEGORIES = [
    "Live Streaming On Demand",
    "Si farà",
    "Recensioni",
    "Serie TV",
    "News",
    "Festival di Cinema",
    "Trailers",
    "Cult Movies",
    "Animazione",
    "Approfondimento",
    "Guide e Film da Vedere",
    "Speciali e Magazine",
    "Rubriche",
    "Interviste",
    "Altro",
]

_MONTH_LABELS_IT = {
    1: "Gennaio", 2: "Febbraio", 3: "Marzo", 4: "Aprile",
    5: "Maggio", 6: "Giugno", 7: "Luglio", 8: "Agosto",
    9: "Settembre", 10: "Ottobre", 11: "Novembre", 12: "Dicembre",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _month_range(n_months: int) -> tuple[str, str]:
    """Restituisce (start_date, end_date) per gli ultimi n_months mesi completi."""
    today = date.today()
    # Ultimo giorno del mese precedente = inizio ricerca dalla fine
    end_year = today.year if today.month > 1 else today.year - 1
    end_month = today.month - 1 if today.month > 1 else 12
    end_day = monthrange(end_year, end_month)[1]
    end_date = date(end_year, end_month, end_day)

    # Primo giorno del mese di inizio
    start_month = end_month - n_months + 1
    start_year = end_year
    while start_month <= 0:
        start_month += 12
        start_year -= 1
    start_date = date(start_year, start_month, 1)

    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


def _yearmonth_label(ym: str) -> str:
    """Converte '202510' → 'Ottobre 2025'."""
    year = int(ym[:4])
    month = int(ym[4:])
    return f"{_MONTH_LABELS_IT.get(month, ym[4:])} {year}"


def _safe_float(val, default=0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def build_category_trend(
    ga4_client,
    category: str,
    n_months: int = DEFAULT_MONTHS,
    top_n: int = DEFAULT_TOP_N,
) -> dict:
    """
    Esegue la query GA4, filtra per categoria e restituisce il trend mensile
    come dizionario strutturato pronto per la serializzazione JSON.
    """
    from map_ga4_categories import map_ga4_categories
    from reports.weekly.weekly_report import contains_si_fara

    start_date, end_date = _month_range(n_months)
    print(f"Periodo analizzato: {start_date} → {end_date}")
    print(f"Categoria: {category}")

    # --- Estrazione dati GA4 ---
    print("Interrogazione GA4 API...")
    df_raw = ga4_client.run_query(
        property_id=PROPERTY_ID,
        dimensions=["pagePath", "yearMonth"],
        metrics=METRICS,
        start_date=start_date,
        end_date=end_date,
    )
    print(f"Righe ricevute dall'API: {len(df_raw)}")

    if df_raw.empty:
        return {
            "category": category,
            "period": {"start": start_date, "end": end_date},
            "months": [],
            "summary": {},
        }

    # --- Conversione tipi ---
    for col in METRICS:
        if col in df_raw.columns:
            df_raw[col] = pd.to_numeric(df_raw[col], errors="coerce").fillna(0)

    # Calcola il tasso di ingaggio corretto a livello di riga
    if "engagedSessions" in df_raw.columns and "sessions" in df_raw.columns:
        df_raw["engagementRate"] = df_raw.apply(
            lambda r: r["engagedSessions"] / r["sessions"] if r["sessions"] > 0 else 0.0,
            axis=1,
        )

    # --- Mapping categoria ---
    df_raw["Categoria"] = df_raw["pagePath"].apply(map_ga4_categories)
    df_raw.loc[df_raw["pagePath"].apply(contains_si_fara), "Categoria"] = "Si farà"
    df_raw.loc[
        df_raw["Categoria"].isin(["Recensioni / In Sala", "Trailers / In Sala"]),
        "Categoria",
    ] = df_raw["Categoria"].str.split(" / ").str[0]

    # --- Filtraggio per categoria ---
    df = df_raw[df_raw["Categoria"] == category].copy()
    print(f"Righe dopo filtro categoria: {len(df)}")

    # Applica soglia minima di visualizzazioni
    if "screenPageViews" in df.columns:
        df = df[df["screenPageViews"] >= MIN_VIEWS]
    print(f"Righe dopo soglia min views ({MIN_VIEWS}): {len(df)}")

    if df.empty:
        return {
            "category": category,
            "period": {"start": start_date, "end": end_date},
            "months": [],
            "summary": {},
        }

    # --- Aggregazione mensile ---
    months_data = []
    sorted_months = sorted(df["yearMonth"].unique())

    for ym in sorted_months:
        month_df = df[df["yearMonth"] == ym]

        total_views = int(month_df["screenPageViews"].sum())
        total_users = int(month_df["activeUsers"].sum()) if "activeUsers" in month_df.columns else None
        total_engaged = month_df["engagedSessions"].sum() if "engagedSessions" in month_df.columns else 0
        total_sessions = month_df["sessions"].sum() if "sessions" in month_df.columns else 0
        avg_engagement = total_engaged / total_sessions if total_sessions > 0 else 0.0
        avg_duration = _safe_float(month_df["averageSessionDuration"].mean())
        article_count = len(month_df)

        # Top N articoli per screenPageViews
        top_articles = (
            month_df.nlargest(top_n, "screenPageViews")
            [["pagePath", "screenPageViews", "engagementRate", "averageSessionDuration"]]
            .assign(url=lambda x: DOMAIN + x["pagePath"])
            .rename(columns={
                "screenPageViews": "pageViews",
                "engagementRate": "engagementRate",
                "averageSessionDuration": "avgSessionDuration",
            })
            .to_dict(orient="records")
        )

        months_data.append({
            "yearMonth": ym,
            "label": _yearmonth_label(ym),
            "totalPageViews": total_views,
            "totalActiveUsers": total_users,
            "totalEngagedSessions": int(total_engaged),
            "totalSessions": int(total_sessions),
            "avgEngagementRate": round(avg_engagement, 4),
            "avgSessionDurationSeconds": round(avg_duration, 1),
            "articleCount": article_count,
            "topArticles": top_articles,
        })

    # --- Riepilogo globale del periodo ---
    total_views_all = sum(m["totalPageViews"] for m in months_data)
    total_users_all = sum(m["totalActiveUsers"] or 0 for m in months_data)
    total_engaged_all = int(df["engagedSessions"].sum()) if "engagedSessions" in df.columns else 0
    total_sessions_all = int(df["sessions"].sum()) if "sessions" in df.columns else 0
    avg_engagement_all = round(total_engaged_all / total_sessions_all, 4) if total_sessions_all > 0 else 0.0
    avg_duration_all = round(df["averageSessionDuration"].mean(), 1)
    total_articles_unique = df["pagePath"].nunique()

    # Mese migliore per views
    best_month = max(months_data, key=lambda m: m["totalPageViews"])

    summary = {
        "totalPageViews": total_views_all,
        "totalActiveUsers": total_users_all,
        "totalEngagedSessions": total_engaged_all,
        "totalSessions": total_sessions_all,
        "avgEngagementRate": avg_engagement_all,
        "avgSessionDurationSeconds": avg_duration_all,
        "uniqueArticlesTracked": total_articles_unique,
        "bestMonth": {
            "yearMonth": best_month["yearMonth"],
            "label": best_month["label"],
            "totalPageViews": best_month["totalPageViews"],
        },
    }

    return {
        "category": category,
        "period": {"start": start_date, "end": end_date},
        "generatedAt": datetime.now().isoformat(),
        "summary": summary,
        "months": months_data,
    }


def save_json(data: dict, output_dir: str, category: str) -> str:
    """Salva il risultato come file JSON e restituisce il path."""
    os.makedirs(output_dir, exist_ok=True)
    safe_cat = category.lower().replace(" ", "_").replace("/", "-")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"category_trend_{safe_cat}_{timestamp}.json"
    path = os.path.join(output_dir, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return path


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def _parse_args():
    parser = argparse.ArgumentParser(
        description="Scarica l'andamento mensile di una categoria editoriale (ultimi N mesi)."
    )
    parser.add_argument(
        "--category",
        default=DEFAULT_CATEGORY,
        help=f"Categoria editoriale da analizzare. Default: '{DEFAULT_CATEGORY}'.\n"
             f"Opzioni valide: {', '.join(VALID_CATEGORIES)}",
    )
    parser.add_argument(
        "--months",
        type=int,
        default=DEFAULT_MONTHS,
        help=f"Numero di mesi precedenti da analizzare. Default: {DEFAULT_MONTHS}.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=DEFAULT_TOP_N,
        help=f"Numero di articoli top per mese da includere nel JSON. Default: {DEFAULT_TOP_N}.",
    )
    parser.add_argument(
        "--output-dir",
        default=os.path.join(_PROJECT_ROOT, "output", "ad_hoc"),
        help="Cartella di output per il JSON. Default: output/ad_hoc/",
    )
    parser.add_argument(
        "--print",
        action="store_true",
        dest="print_output",
        help="Stampa il JSON anche su stdout.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    if args.category not in VALID_CATEGORIES:
        print(f"ATTENZIONE: categoria '{args.category}' non riconosciuta.")
        print(f"Categorie valide: {', '.join(VALID_CATEGORIES)}")
        sys.exit(1)

    from ga4_api.ga4_api import Ga4Client

    ga4_client = Ga4Client()

    result = build_category_trend(
        ga4_client=ga4_client,
        category=args.category,
        n_months=args.months,
        top_n=args.top_n,
    )

    output_path = save_json(result, args.output_dir, args.category)
    print(f"\nJSON salvato in: {output_path}")

    if args.print_output:
        print(json.dumps(result, ensure_ascii=False, indent=2))

    # Stampa riepilogo a schermo
    s = result.get("summary", {})
    print("\n=== RIEPILOGO ===")
    print(f"Categoria       : {result['category']}")
    print(f"Periodo         : {result['period']['start']} → {result['period']['end']}")
    print(f"Page Views tot. : {s.get('totalPageViews', 'N/A'):,}")
    print(f"Utenti attivi   : {s.get('totalActiveUsers', 'N/A'):,}")
    print(f"Engagement rate : {s.get('avgEngagementRate', 'N/A'):.1%}")
    print(f"Durata media    : {s.get('avgSessionDurationSeconds', 'N/A'):.0f}s")
    print(f"Articoli unici  : {s.get('uniqueArticlesTracked', 'N/A')}")
    bm = s.get("bestMonth", {})
    if bm:
        print(f"Mese migliore   : {bm.get('label')} ({bm.get('totalPageViews', 0):,} views)")

    print("\n=== TREND MENSILE ===")
    for m in result.get("months", []):
        print(
            f"  {m['label']:20s}  views={m['totalPageViews']:>6,}  "
            f"utenti={m['totalActiveUsers'] or 0:>5,}  "
            f"engagement={m['avgEngagementRate']:.1%} ({m.get('totalEngagedSessions',0):,}/{m.get('totalSessions',0):,})  "
            f"articoli={m['articleCount']}"
        )
