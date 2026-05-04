# CLAUDE.md — Taxi Drivers Analytics Project

Guida rapida per AI assistant. Leggere prima di esplorare il codebase.

---

## Progetto

Analytics dashboard + reporting per **taxidrivers.it** (cinema/film magazine italiano).
Dati da **Google Analytics 4** → ETL → report Excel/CSV + dashboard Dash.

**Property ID GA4:** `394327334`  
**Dominio:** `https://taxidrivers.it`  
**Python:** 3.12.3 — venv in `.td_ds_venv/`

---

## Struttura Chiave

```
ga4_api/ga4_api.py          → Ga4Client (OAuth2 + BetaAnalyticsDataClient)
etl/page_and_screen_etl.py  → PageAndScreenETLFactory (pulizia dati GA4)
reports/map_ga4_categories.py → map_ga4_categories(path) → categoria
reports/ad_hoc_reports/sandra_report.py → report top 100 articoli settimanale
reports/run_reports.ps1     → entry point PowerShell per tutti i report
report_config.py            → costanti di path centralizzate
config/settings.py          → settings applicazione
Dashboards/dashboard/       → app Dash (layout + callbacks)
notebooks/                  → Jupyter notebook analisi
reports/ad_hoc_reports/     → report one-off e analisi
```

---

## Comandi Principali

```powershell
# Attivare venv
.\.td_ds_venv\Scripts\Activate.ps1

# Eseguire report Sandra (top 100 articoli, default 7 giorni)
.\reports\run_reports.ps1 sandra
.\reports\run_reports.ps1 sandra -Days 14
.\reports\run_reports.ps1 sandra -StartDate 2026-04-06 -EndDate 2026-04-12

# Dashboard
python app.py
```

---

## Pattern Ricorrenti

### Caricare dati da GA4
```python
from ga4_api.ga4_api import Ga4Client
ga4 = Ga4Client()
df = ga4.run_query(
    property_id='394327334',
    dimensions=['pagePath', 'date'],
    metrics=['screenPageViews', 'activeUsers', 'engagedSessions', 'sessions', 'averageSessionDuration'],
    start_date='2026-04-06',
    end_date='2026-04-12'
)
```

### ETL pulizia (rimuove non-.html, rimuove homepage)
```python
from etl.page_and_screen_etl import PageAndScreenETLFactory
etl = PageAndScreenETLFactory.get_etl('en', df=df)
etl.apply_transformations()
df = etl.df
```

### Mappare categorie
```python
from map_ga4_categories import map_ga4_categories
df['category'] = df['pagePath'].apply(map_ga4_categories)
# Categorie: News, Sì farà, Recensioni, In Sala, Festival di Cinema,
#            Serie TV, Anticipazioni, Live Streaming On Demand, Trailers,
#            Speciali e Magazine, Interviste, Rubriche, Cult Movies,
#            Animazione, Guide e Film da Vedere, Altro
```

### Performance bucket (quintili)
```python
# Già implementato in sandra_report.py (commit 3d06717)
from reports.ad_hoc_reports.sandra_report import assign_performance_buckets
# Aggiunge colonne: screenPageViews_fascia, engagementRate_fascia, averageSessionDuration_fascia
# Labels: Basso, Medio-basso, Nella media, Medio-alto, Alto
```

### Calcolo engagement rate
```python
df['engagementRate'] = (df['engagedSessions'] / df['sessions'] * 100).round(2)
```

---

## Metriche GA4 Disponibili

| Metrica | Descrizione |
|---------|-------------|
| `screenPageViews` | Page views |
| `activeUsers` | Utenti attivi |
| `sessions` | Sessioni totali |
| `engagedSessions` | Sessioni con engagement |
| `averageSessionDuration` | Durata media (secondi) |
| `engagementRate` | % sessioni con engagement (calcolato) |

**Dimensioni comuni:** `pagePath`, `date`, `sessionDefaultChannelGroup`, `deviceCategory`

---

## Notebook Rilevanti

| File | Contenuto |
|------|-----------|
| `notebooks/category_views_trend_dynamic.ipynb` | Analisi categorie per date range arbitrario (param: `START_DATE`, `END_DATE`) |
| `reports/ad_hoc_reports/category_views_trend.ipynb` | Trend mensili Oct 2025–Mar 2026 |
| `reports/ad_hoc_reports/sandra_eda.ipynb` | EDA report Sandra |

**Per cambiare periodo nel notebook dinamico:** modifica solo la cella "Configuration" (START_DATE / END_DATE / GRANULARITY).

---

## File di Output

```
output/                     → output generale
output/weekly/              → report settimanali CSV/Excel
output/monthly/             → report mensili
reports/output/             → output report ad-hoc
```

---

## Dipendenze Chiave

```
pandas==2.3.3
numpy==2.0.0
matplotlib==3.10.0
seaborn==0.13.2
scipy==1.15.2
google-analytics-data==0.18.19
openpyxl==3.1.5
psycopg2-binary==2.9.11    # PostgreSQL (articles_db)
dash / plotly               # Dashboard
```

**NON installato:** `ipywidgets` — usare parametri hardcoded nei notebook invece di widget interattivi.

---

## Gotcha e Note

- **Path setup nei notebook:** `sys.path.insert(0, str(Path.cwd().parent))` dal folder `notebooks/`
- **Token GA4:** `token.pickle` presente in più cartelle (ga4_api/, reports/, etc.) — OAuth2 cached
- **ETL stampa sempre** le righe droppate su stdout (comportamento normale, non errore)
- **Mojibake fix:** `sandra_report.py` ha `_fix_mojibake_text()` per correggere encoding latin-1/cp1252
- **`articles_db`:** PostgreSQL separato via Docker (`docker-compose.yml`) — non necessario per report base
- **`map_ga4_categories` in due posti:** `reports/map_ga4_categories.py` (principale) e variante in `sandra_report.py` (con keyword leggermente diverse per Recensioni)

---

## Stato Attuale (Aprile 2026)

- ✅ `sandra_report.py`: bucket performance per quintile (commit 3d06717)
- ✅ `category_views_trend_dynamic.ipynb`: notebook parametrizzato funzionante
- ✅ Analisi 06-12 aprile eseguita (14,729 PV, 16 categorie)
- 🔄 Dashboard Dash: in sviluppo attivo (`Dashboards/dashboard/`)
- 🔄 `articles_db`: pipeline scraping articoli da WordPress XML + PostgreSQL
