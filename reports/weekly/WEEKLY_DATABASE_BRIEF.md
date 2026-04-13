# Brief di Progetto — Database Weekly Results

## 1) Brainstorming iniziale (fase 1)

### Obiettivo pratico
Costruire un database unico che storicizzi i risultati del report settimanale (`weekly`) e renda semplici:
- analisi top articoli (assolute e per categoria),
- trend nel tempo per views/engagement,
- monitoraggio distribuzione performance (editorial score, Gini, Lorenz),
- export verso dashboard e report editoriali.

### Cosa emerge dal notebook `weekly_nb`
Nel notebook vengono usati in modo ricorrente:
- colonne articolo: `Title`, `Author`, `Categoria`, `pagePath`, `Publication Date`;
- metriche: `screenPageViews`, `engagementRate`, `averageSessionDuration`, `editorial_score`;
- analisi aggregate: storico per categoria (`views_by_category_history.csv`, `engagement_by_category_history.csv`);
- metrica di concentrazione traffico: `gini_coefficient.csv`.

### Scelte architetturali candidate
- **Opzione A (MVP veloce):** SQLite locale + pipeline Python.
- **Opzione B (scalabile):** PostgreSQL + job schedulato.
- **Opzione C (analitica):** DuckDB + parquet (ottimo per analisi batch).

Per coerenza con il progetto (già presente componente Postgres), scelta consigliata: **PostgreSQL**.

---

## 2) Brief di progetto (fase 2)

## Vision
Creare un "Weekly Analytics Store" affidabile, incrementale e query-friendly, che raccolga ogni esecuzione weekly in un modello dati normalizzato e storicizzato.

## Scope MVP
1. Ingestione dati dal file `output/weekly/weekly_report.csv` a ogni run.
2. Persistenza articoli e metriche per data di run.
3. Persistenza aggregati per categoria (views/engagement).
4. Persistenza indice Gini settimanale.
5. Query base per top articoli, trend categoria, confronto week-over-week.

## Non-obiettivi MVP
- Real-time streaming.
- Modelli predittivi avanzati.
- BI enterprise completa (da fare in fase 2).

## Modello dati proposto

### Tabella `dim_article`
Anagrafica articolo deduplicata per URL/path.
- `article_id` (PK)
- `page_path` (UNIQUE)
- `title`
- `author`
- `category`
- `publication_date`
- `first_seen_at`
- `last_seen_at`

### Tabella `fact_weekly_article_metrics`
Snapshot metriche per articolo e per run settimanale.
- `run_id` (FK)
- `article_id` (FK)
- `screen_page_views`
- `engagement_rate`
- `average_session_duration`
- `editorial_score`
- `content_segment` (se disponibile)
- PK composta: (`run_id`, `article_id`)

### Tabella `dim_run`
Metadati della singola esecuzione weekly.
- `run_id` (PK)
- `run_datetime`
- `start_date`
- `end_date`
- `source_file`
- `rows_ingested`
- `status`

### Tabella `fact_weekly_category_metrics`
Aggregati categoria per run (dal notebook).
- `run_id` (FK)
- `category`
- `views_sum`
- `engagement_median`
- PK composta: (`run_id`, `category`)

### Tabella `fact_weekly_distribution_metrics`
Metriche globali distribuzione traffico.
- `run_id` (FK)
- `gini_coefficient`
- `articles_count`
- `notes`

## Flusso ETL
1. **Extract**
   - Leggi `weekly_report.csv`.
   - Leggi (opzionale) file storici categoria e gini già usati dal notebook.
2. **Transform**
   - Cast tipi (`Publication Date` datetime, metriche numeriche).
   - Normalizza testo (`title`, `author`, `category`) e encoding.
   - Deduplica per `pagePath`.
3. **Load**
   - Inserisci riga in `dim_run`.
   - Upsert in `dim_article`.
   - Insert in facts articolo/categoria/distribuzione.

## Regole qualità dati
- No duplicati per (`run_id`, `article_id`).
- `screen_page_views >= 0`.
- `engagement_rate` tra 0 e 1 (o 0-100 con normalizzazione coerente).
- Campi testuali sanitizzati (mojibake fix) prima del load.
- Logging dei record falliti + retry finale scraping (già presente nei report script).

## Query operative da supportare
- Top N articoli per `screen_page_views` in un dato run.
- Top N per categoria e metrica (`engagement_rate`, `editorial_score`).
- Trend views per categoria su più run.
- Evoluzione `gini_coefficient` nel tempo.

## Deliverable fase 1
1. Schema SQL iniziale (DDL).
2. Script Python di ingestione weekly (`ingest_weekly_to_db.py`).
3. Script validazione qualità dati.
4. 5 query SQL "starter" per analisi editoriale.

## Piano sintetico
- **Settimana 1:** schema DB + loader base.
- **Settimana 2:** aggregati categoria + gini + quality checks.
- **Settimana 3:** hardening, logging, query pack, documentazione.

## Rischi e mitigazioni
- **Schema CSV variabile** → mapping colonne configurabile.
- **Encoding sporco** → normalizzazione centralizzata in pipeline.
- **Outlier metriche** → controlli e flag di anomalia.

## Next step consigliato
Implementare subito il DDL e un loader MVP che carica solo `dim_run`, `dim_article`, `fact_weekly_article_metrics`; poi estendere ad aggregati categoria e Gini.
