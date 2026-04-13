"""
all_categories_trend.py
-----------------------
Analisi del trend mensile di TUTTE le categorie editoriali negli ultimi N mesi.
Esegue UNA sola query GA4 e processa tutte le categorie dallo stesso dataframe.

Output nella cartella output/ad_hoc/all_categories_trend_<timestamp>/:
  - all_categories_trend.json      → struttura completa nested (category → months → top articles)
  - monthly_flat.csv               → una riga per (categoria, mese): ottimale per pandas / BI
  - summary.csv                    → una riga per categoria: confronto rapido tra categorie

Uso:
    python reports/all_categories_trend.py
    python reports/all_categories_trend.py --months 6 --top-n 10
"""

import argparse
import json
import os
import sys
from calendar import monthrange
from datetime import date, datetime

import pandas as pd

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, ".."))
for _p in (_SCRIPT_DIR, _PROJECT_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ---------------------------------------------------------------------------
# Costanti (allineate a category_trend.py)
# ---------------------------------------------------------------------------
PROPERTY_ID = "394327334"
DOMAIN = "https://taxidrivers.it"
DEFAULT_MONTHS = 6
DEFAULT_TOP_N = 10
MIN_VIEWS = 5

METRICS = [
    "screenPageViews",
    "activeUsers",
    "engagedSessions",
    "sessions",
    "averageSessionDuration",
]

_MONTH_LABELS_IT = {
    1: "Gennaio", 2: "Febbraio", 3: "Marzo", 4: "Aprile",
    5: "Maggio", 6: "Giugno", 7: "Luglio", 8: "Agosto",
    9: "Settembre", 10: "Ottobre", 11: "Novembre", 12: "Dicembre",
}


# ---------------------------------------------------------------------------
# Helpers condivisi
# ---------------------------------------------------------------------------

def _month_range(n_months: int) -> tuple[str, str]:
    today = date.today()
    end_year = today.year if today.month > 1 else today.year - 1
    end_month = today.month - 1 if today.month > 1 else 12
    end_day = monthrange(end_year, end_month)[1]
    end_date = date(end_year, end_month, end_day)

    start_month = end_month - n_months + 1
    start_year = end_year
    while start_month <= 0:
        start_month += 12
        start_year -= 1
    start_date = date(start_year, start_month, 1)
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


def _yearmonth_label(ym: str) -> str:
    year = int(ym[:4])
    month = int(ym[4:])
    return f"{_MONTH_LABELS_IT.get(month, ym[4:])} {year}"


# ---------------------------------------------------------------------------
# Fetch + enrich (una sola volta per tutte le categorie)
# ---------------------------------------------------------------------------

def fetch_and_prepare(ga4_client, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Scarica i dati da GA4 e applica mapping categorie.
    Restituisce il dataframe grezzo arricchito, pronto per il filtraggio per categoria.
    """
    from map_ga4_categories import map_ga4_categories
    from reports.weekly.weekly_report import contains_si_fara

    print(f"Interrogazione GA4 API ({start_date} → {end_date})...")
    df = ga4_client.run_query(
        property_id=PROPERTY_ID,
        dimensions=["pagePath", "yearMonth"],
        metrics=METRICS,
        start_date=start_date,
        end_date=end_date,
    )
    print(f"Righe ricevute: {len(df)}")

    if df.empty:
        return df

    # Conversione tipi metriche
    for col in METRICS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Engagement rate corretto: engagedSessions / sessions per riga
    df["engagementRate"] = df.apply(
        lambda r: r["engagedSessions"] / r["sessions"] if r["sessions"] > 0 else 0.0,
        axis=1,
    )

    # Mapping categorie
    df["Categoria"] = df["pagePath"].apply(map_ga4_categories)
    df.loc[df["pagePath"].apply(contains_si_fara), "Categoria"] = "Si farà"
    # Normalizza varianti "Recensioni / In Sala" → "Recensioni", "Trailers / In Sala" → "Trailers"
    df.loc[df["Categoria"] == "Recensioni / In Sala", "Categoria"] = "Recensioni"
    df.loc[df["Categoria"] == "Trailers / In Sala", "Categoria"] = "Trailers"

    # Soglia minima views
    df = df[df["screenPageViews"] >= MIN_VIEWS].copy()
    print(f"Righe dopo soglia min views ({MIN_VIEWS}): {len(df)}")
    return df


# ---------------------------------------------------------------------------
# Aggregazione per singola categoria
# ---------------------------------------------------------------------------

def _aggregate_category(df_cat: pd.DataFrame, top_n: int) -> tuple[list, dict]:
    """
    Dato il dataframe filtrato per una categoria, restituisce
    (months_data: list, summary: dict).
    """
    months_data = []
    sorted_months = sorted(df_cat["yearMonth"].unique())

    for ym in sorted_months:
        month_df = df_cat[df_cat["yearMonth"] == ym]

        total_views = int(month_df["screenPageViews"].sum())
        total_users = int(month_df["activeUsers"].sum())
        total_engaged = int(month_df["engagedSessions"].sum())
        total_sessions = int(month_df["sessions"].sum())
        avg_engagement = total_engaged / total_sessions if total_sessions > 0 else 0.0
        avg_duration = float(month_df["averageSessionDuration"].mean())
        article_count = len(month_df)

        top_df = month_df.nlargest(top_n, "screenPageViews")[
            ["pagePath", "screenPageViews", "engagedSessions", "sessions", "averageSessionDuration"]
        ].copy()
        top_df["url"] = DOMAIN + top_df["pagePath"]
        top_df.rename(columns={
            "screenPageViews": "pageViews",
            "averageSessionDuration": "avgSessionDurationSeconds",
        }, inplace=True)
        top_articles = top_df.to_dict(orient="records")

        months_data.append({
            "yearMonth": ym,
            "label": _yearmonth_label(ym),
            "totalPageViews": total_views,
            "totalActiveUsers": total_users,
            "totalEngagedSessions": total_engaged,
            "totalSessions": total_sessions,
            "avgEngagementRate": round(avg_engagement, 4),
            "avgSessionDurationSeconds": round(avg_duration, 1),
            "articleCount": article_count,
            "topArticles": top_articles,
        })

    if not months_data:
        return months_data, {}

    total_views_all = sum(m["totalPageViews"] for m in months_data)
    total_users_all = sum(m["totalActiveUsers"] for m in months_data)
    total_engaged_all = int(df_cat["engagedSessions"].sum())
    total_sessions_all = int(df_cat["sessions"].sum())
    avg_engagement_all = round(total_engaged_all / total_sessions_all, 4) if total_sessions_all > 0 else 0.0
    avg_duration_all = round(float(df_cat["averageSessionDuration"].mean()), 1)
    unique_articles = int(df_cat["pagePath"].nunique())

    best_month = max(months_data, key=lambda m: m["totalPageViews"])
    worst_month = min(months_data, key=lambda m: m["totalPageViews"])

    # Tendenza: regressione lineare semplice su totalPageViews (slope normalizzata)
    views_series = [m["totalPageViews"] for m in months_data]
    n = len(views_series)
    if n >= 2:
        x_mean = (n - 1) / 2
        y_mean = sum(views_series) / n
        numerator = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(views_series))
        denominator = sum((i - x_mean) ** 2 for i in range(n))
        slope = numerator / denominator if denominator != 0 else 0.0
        # tendenza come % del valore medio mensile
        trend_pct = round(slope / y_mean * 100, 2) if y_mean != 0 else 0.0
    else:
        trend_pct = 0.0

    summary = {
        "totalPageViews": total_views_all,
        "totalActiveUsers": total_users_all,
        "totalEngagedSessions": total_engaged_all,
        "totalSessions": total_sessions_all,
        "avgEngagementRate": avg_engagement_all,
        "avgSessionDurationSeconds": avg_duration_all,
        "uniqueArticlesTracked": unique_articles,
        "monthsAnalyzed": n,
        "trendPctPerMonth": trend_pct,  # >0 crescita, <0 calo
        "bestMonth": {
            "yearMonth": best_month["yearMonth"],
            "label": best_month["label"],
            "totalPageViews": best_month["totalPageViews"],
        },
        "worstMonth": {
            "yearMonth": worst_month["yearMonth"],
            "label": worst_month["label"],
            "totalPageViews": worst_month["totalPageViews"],
        },
    }
    return months_data, summary


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def _build_monthly_flat(all_results: list) -> pd.DataFrame:
    """
    Costruisce un DataFrame piatto (una riga per categoria+mese).
    Colonne utili per analisi pandas / BI.
    """
    rows = []
    for cat_data in all_results:
        cat = cat_data["category"]
        period_start = cat_data["period"]["start"]
        period_end = cat_data["period"]["end"]
        for m in cat_data.get("months", []):
            rows.append({
                "category": cat,
                "yearMonth": m["yearMonth"],
                "monthLabel": m["label"],
                "periodStart": period_start,
                "periodEnd": period_end,
                "totalPageViews": m["totalPageViews"],
                "totalActiveUsers": m["totalActiveUsers"],
                "totalEngagedSessions": m["totalEngagedSessions"],
                "totalSessions": m["totalSessions"],
                "avgEngagementRate": m["avgEngagementRate"],
                "avgSessionDurationSeconds": m["avgSessionDurationSeconds"],
                "articleCount": m["articleCount"],
            })
    return pd.DataFrame(rows)


def _build_summary_df(all_results: list) -> pd.DataFrame:
    """
    Costruisce un DataFrame di riepilogo (una riga per categoria).
    """
    rows = []
    for cat_data in all_results:
        s = cat_data.get("summary", {})
        if not s:
            continue
        rows.append({
            "category": cat_data["category"],
            "totalPageViews": s.get("totalPageViews", 0),
            "totalActiveUsers": s.get("totalActiveUsers", 0),
            "totalEngagedSessions": s.get("totalEngagedSessions", 0),
            "totalSessions": s.get("totalSessions", 0),
            "avgEngagementRate": s.get("avgEngagementRate", 0),
            "avgSessionDurationSeconds": s.get("avgSessionDurationSeconds", 0),
            "uniqueArticlesTracked": s.get("uniqueArticlesTracked", 0),
            "monthsAnalyzed": s.get("monthsAnalyzed", 0),
            "trendPctPerMonth": s.get("trendPctPerMonth", 0),
            "bestMonth": s.get("bestMonth", {}).get("label", ""),
            "bestMonthViews": s.get("bestMonth", {}).get("totalPageViews", 0),
            "worstMonth": s.get("worstMonth", {}).get("label", ""),
            "worstMonthViews": s.get("worstMonth", {}).get("totalPageViews", 0),
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("totalPageViews", ascending=False).reset_index(drop=True)
    return df


def save_outputs(all_results: list, output_dir: str) -> dict[str, str]:
    """Salva JSON, CSV flat e CSV summary. Restituisce dict {tipo: path}."""
    os.makedirs(output_dir, exist_ok=True)
    paths = {}

    # 1. JSON completo
    json_path = os.path.join(output_dir, "all_categories_trend.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)
    paths["json"] = json_path

    # 2. CSV piatto mensile
    flat_df = _build_monthly_flat(all_results)
    flat_path = os.path.join(output_dir, "monthly_flat.csv")
    flat_df.to_csv(flat_path, index=False, encoding="utf-8-sig")
    paths["monthly_flat_csv"] = flat_path

    # 3. CSV summary per categoria
    summary_df = _build_summary_df(all_results)
    summary_path = os.path.join(output_dir, "summary.csv")
    summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
    paths["summary_csv"] = summary_path

    return paths


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_all_categories_trend(
    ga4_client,
    n_months: int = DEFAULT_MONTHS,
    top_n: int = DEFAULT_TOP_N,
    output_dir: str = None,
) -> tuple[list, dict[str, str]]:
    """
    Punto di ingresso principale. Ritorna (all_results, output_paths).
    """
    from category_trend import VALID_CATEGORIES  # riusa la lista canonica

    start_date, end_date = _month_range(n_months)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if output_dir is None:
        output_dir = os.path.join(
            _PROJECT_ROOT, "output", "ad_hoc",
            f"all_categories_trend_{timestamp}"
        )

    # Una sola query GA4 per tutto il periodo e tutte le pagine
    df_all = fetch_and_prepare(ga4_client, start_date, end_date)

    all_categories = sorted(df_all["Categoria"].unique()) if not df_all.empty else VALID_CATEGORIES

    all_results = []
    for cat in all_categories:
        print(f"\n>>> Categoria: {cat}")
        df_cat = df_all[df_all["Categoria"] == cat].copy() if not df_all.empty else pd.DataFrame()
        n_rows = len(df_cat)
        print(f"    Righe: {n_rows}")

        if df_cat.empty:
            all_results.append({
                "category": cat,
                "period": {"start": start_date, "end": end_date},
                "generatedAt": timestamp,
                "summary": {},
                "months": [],
            })
            continue

        months_data, summary = _aggregate_category(df_cat, top_n)
        all_results.append({
            "category": cat,
            "period": {"start": start_date, "end": end_date},
            "generatedAt": timestamp,
            "summary": summary,
            "months": months_data,
        })

    output_paths = save_outputs(all_results, output_dir)
    return all_results, output_paths


# ---------------------------------------------------------------------------
# Stampa riepilogo a schermo
# ---------------------------------------------------------------------------

def print_summary(all_results: list) -> None:
    rows = []
    for cat_data in all_results:
        s = cat_data.get("summary", {})
        if not s:
            continue
        trend = s.get("trendPctPerMonth", 0)
        trend_sym = "▲" if trend > 2 else ("▼" if trend < -2 else "→")
        rows.append((
            cat_data["category"],
            s.get("totalPageViews", 0),
            s.get("totalActiveUsers", 0),
            s.get("avgEngagementRate", 0),
            s.get("uniqueArticlesTracked", 0),
            trend,
            trend_sym,
        ))

    rows.sort(key=lambda r: r[1], reverse=True)

    print("\n" + "=" * 95)
    print(f"{'CATEGORIA':<30} {'VIEWS':>8} {'UTENTI':>8} {'ENGAG.':>8} {'ART.':>6} {'TREND/MESE':>12}")
    print("=" * 95)
    for cat, views, users, eng, art, trend, sym in rows:
        print(
            f"{cat:<30} {views:>8,} {users:>8,} {eng:>8.1%} {art:>6,} "
            f"{sym} {trend:>+7.1f}%"
        )
    print("=" * 95)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args():
    parser = argparse.ArgumentParser(
        description="Trend mensile di tutte le categorie editoriali (unica query GA4)."
    )
    parser.add_argument("--months", type=int, default=DEFAULT_MONTHS,
                        help=f"Mesi precedenti da analizzare. Default: {DEFAULT_MONTHS}.")
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N,
                        help=f"Top N articoli per categoria per mese. Default: {DEFAULT_TOP_N}.")
    parser.add_argument("--output-dir", default=None,
                        help="Cartella di output (default: output/ad_hoc/all_categories_trend_<ts>/).")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    from ga4_api.ga4_api import Ga4Client
    ga4_client = Ga4Client()

    all_results, output_paths = run_all_categories_trend(
        ga4_client=ga4_client,
        n_months=args.months,
        top_n=args.top_n,
        output_dir=args.output_dir,
    )

    print_summary(all_results)

    print("\n=== FILE SALVATI ===")
    for kind, path in output_paths.items():
        print(f"  {kind:<20} {path}")
