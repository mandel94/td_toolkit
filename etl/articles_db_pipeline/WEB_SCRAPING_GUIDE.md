# Web Scraping Module - TaxiDrivers.it Articles

📚 **Guida completa all'utilizzo del modulo di web scraping per l'estrazione automatica degli articoli da TaxiDrivers.it**

## 📋 Indice

- [Panoramica](#panoramica)
- [Architettura](#architettura)
- [Installazione](#installazione)
- [Utilizzo](#utilizzo)
  - [Esecuzione Manuale](#esecuzione-manuale)
  - [Esecuzione Schedulata](#esecuzione-schedulata)
- [Configurazione](#configurazione)
- [Database](#database)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

---

## 🎯 Panoramica

Questo modulo implementa un sistema completo di **ETL (Extract, Transform, Load)** per lo scraping automatico degli articoli dal sito TaxiDrivers.it. Il sistema è progettato seguendo le best practices di data engineering e software development.

### Caratteristiche Principali

✅ **Web Scraping Professionale**
- Estrazione strutturata da pagina archivio e pagine dettaglio articoli
- Rate limiting e gestione intelligente delle richieste
- Retry logic per gestione errori di rete
- User agent configurabile

✅ **Pattern Observer**
- Monitoraggio in tempo reale del progresso di scraping
- Notifiche su batch completati, errori, e metriche
- Logging dettagliato con loguru

✅ **Batch Processing**
- Processamento in gruppi di 100 articoli (configurabile)
- Pausa di 2 minuti tra batch per rispetto del sito
- Gestione memoria ottimizzata

✅ **Persistenza Database**
- Salvataggio in PostgreSQL con schema dimensionale
- Tabella dedicata per raw scraped data
- Aggiornamento automatico tabelle dimensionali
- Supporto per versionamento articoli

✅ **Scheduling Automatico**
- Esecuzione automatica settimanale (venerdì di default)
- Logging di tutte le esecuzioni
- Salvataggio risultati in formato JSON

---

## 🏗️ Architettura

### Struttura Moduli

```
etl/articles_db_pipeline/
├── scrapers/                          # Modulo scraping centralizzato
│   ├── base_scraper.py               # Classe base con Observer pattern
│   ├── archive_scraper.py            # Scraper pagina archivio
│   ├── article_detail_scraper.py     # Scraper dettagli articoli
│   └── selectors.py                  # CSS selectors centralizzati
│
├── extractors/
│   └── web_scraping_extractor.py     # Extractor principale
│
├── loaders/
│   └── scraped_articles_loader.py    # Loader per database
│
├── models/
│   └── scraped_article.py            # Modelli Pydantic
│
├── web_scraping_pipeline.py          # Pipeline completa
├── scheduler.py                       # Scheduler settimanale
└── web_scraping_cli.py               # Command-line interface
```

### Flusso Dati

```
┌─────────────────────────────────────────────────────────────────┐
│                     WEB SCRAPING PIPELINE                        │
└─────────────────────────────────────────────────────────────────┘

1. EXTRACTION
   ┌──────────────────┐
   │ Archive Page     │  → Lista articoli (titolo, categoria, URL)
   │ Scraping         │
   └────────┬─────────┘
            │
            ↓
   ┌──────────────────┐
   │ Article Detail   │  → Dettagli per ogni articolo
   │ Scraping         │     (sottotitolo, autore, data, body HTML/text)
   │ (Batched)        │
   └────────┬─────────┘
            │
2. LOADING  ↓
   ┌──────────────────┐
   │ Save to          │  → scraped_articles_raw table
   │ PostgreSQL       │
   └────────┬─────────┘
            │
3. UPDATE   ↓
   ┌──────────────────┐
   │ Update           │  → dim_articles, dim_authors, dim_categories
   │ Dimensional      │
   │ Tables           │
   └──────────────────┘
```

### Pattern Observer

Il sistema utilizza il **pattern Observer** per monitorare il progresso in tempo reale:

```python
class ScraperObserver(ABC):
    def on_progress_update(progress: ScrapingProgress)
    def on_batch_complete(batch_number: int, batch_size: int)
    def on_error(error: Exception, url: str)
```

---

## 📦 Installazione

### 1. Prerequisiti

- Python 3.8+
- PostgreSQL 15+
- Database articles_db configurato e in esecuzione

### 2. Installare Dipendenze

```bash
cd etl/articles_db_pipeline
pip install -r requirements.txt
```

Le dipendenze principali sono:
- `beautifulsoup4` - parsing HTML
- `requests` - HTTP client
- `sqlalchemy` - ORM database
- `pydantic` - validazione dati
- `loguru` - logging avanzato
- `schedule` - scheduling automatico

### 3. Configurare Database

Assicurarsi che il database sia in esecuzione:

```bash
cd articles_db
docker-compose up -d
```

Il modulo creerà automaticamente la tabella `scraped_articles_raw` al primo utilizzo.

---

## 🚀 Utilizzo

### Esecuzione Manuale

#### 1. Scraping Completo (CLI)

```bash
# Esegui scraping completo con impostazioni default
python web_scraping_cli.py scrape

# Limita a 50 articoli (per test)
python web_scraping_cli.py scrape --limit 50

# Personalizza parametri
python web_scraping_cli.py scrape --delay 3.0 --batch-size 50 --batch-pause 180

# Salva risultati in JSON
python web_scraping_cli.py scrape --output results.json

# Logging verboso
python web_scraping_cli.py -v scrape
```

#### 2. Verifica Status

```bash
# Mostra stato database e ultimo scraping
python web_scraping_cli.py status
```

Output esempio:
```
================================================================================
PIPELINE STATUS
================================================================================
Database connected: True
Total scraped articles: 1250
Latest scrape: 2025-02-13T02:15:30
================================================================================
```

#### 3. Aggiorna Solo Tabelle Dimensionali

```bash
# Sincronizza dim_articles, dim_authors, dim_categories
python web_scraping_cli.py update-dim
```

### Esecuzione Schedulata

#### 1. Avvio Scheduler (Linux/Mac)

```bash
# Esegui ogni venerdì alle 02:00 (default)
python scheduler.py

# Personalizza giorno e ora
python scheduler.py --day monday --time 03:30

# Test: esegui immediatamente
python scheduler.py --run-now
```

#### 2. Avvio Scheduler (Windows)

**Opzione A: PowerShell Script**

Usa lo script fornito:
```powershell
.\Start-WebScraping-Scheduler.ps1
```

**Opzione B: Task Scheduler di Windows**

1. Apri "Task Scheduler" (Utilità di pianificazione)
2. Crea nuova attività base
3. Trigger: Settimanale, Venerdì, 02:00
4. Azione: Avvia programma
   - Programma: `python.exe`
   - Argomenti: `c:\path\to\scheduler.py`
   - Directory: `c:\path\to\etl\articles_db_pipeline`

#### 3. Esecuzione come Servizio (Linux)

Crea un servizio systemd:

```bash
# Crea file /etc/systemd/system/taxidrivers-scraper.service
[Unit]
Description=TaxiDrivers.it Web Scraping Scheduler
After=network.target postgresql.service

[Service]
Type=simple
User=your_user
WorkingDirectory=/path/to/etl/articles_db_pipeline
ExecStart=/usr/bin/python3 scheduler.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target

# Abilita e avvia servizio
sudo systemctl enable taxidrivers-scraper
sudo systemctl start taxidrivers-scraper
sudo systemctl status taxidrivers-scraper
```

### Utilizzo Programmatico

```python
from etl.articles_db_pipeline.web_scraping_pipeline import WebScrapingPipeline

# Crea pipeline
pipeline = WebScrapingPipeline(
    base_url="https://www.taxidrivers.it",
    delay_between_requests=2.0,
    batch_size=100,
    batch_pause_duration=120
)

# Esegui scraping completo
results = pipeline.run_full_pipeline(
    archive_url="/archivio",
    limit=None,  # Tutti gli articoli
    update_dim_tables=True
)

# Verifica risultati
if results['success']:
    print(f"Estratti {results['extraction']['successful']} articoli")
    print(f"Caricati {results['loading']['loaded']} articoli")
```

---

## ⚙️ Configurazione

### Parametri Scraping

| Parametro | Default | Descrizione |
|-----------|---------|-------------|
| `base_url` | `https://www.taxidrivers.it` | URL base sito |
| `delay_between_requests` | `2.0` | Secondi tra richieste HTTP |
| `batch_size` | `100` | Articoli per batch |
| `batch_pause_duration` | `120` | Secondi pausa tra batch |
| `timeout` | `30` | Timeout richieste HTTP (sec) |
| `max_retries` | `3` | Tentativi retry per errori |

### CSS Selectors

I selectors CSS sono centralizzati in `scrapers/selectors.py` per facilitare la manutenzione:

```python
class TaxiDriversSelectors:
    # Archive page
    ARCHIVE_ARTICLE_CONTAINER = "mvp-blog-story-wrap"
    ARCHIVE_TITLE_TAG = "h2"
    ARCHIVE_CATEGORY_CLASS = "mvp-cd-cat"
    
    # Article detail
    DETAIL_TITLE_CLASS = "mvp-post-title"
    DETAIL_SUBTITLE_CLASS = "mvp-post-excerpt"
    DETAIL_AUTHOR_REL = "author"
    DETAIL_DATE_CLASS = "post-date"
    DETAIL_BODY_CLASS = "mvp-content-main"
```

**⚠️ Importante:** Se il layout del sito cambia, aggiornare solo questo file!

### Scheduler

Configura lo scheduler in `scheduler.py`:

```python
scheduler = WebScrapingScheduler(
    schedule_day="friday",      # Giorno settimana
    schedule_time="02:00",      # Ora esecuzione
    log_dir="./logs"            # Directory log
)
```

---

## 💾 Database

### Schema Tabella Raw

```sql
CREATE TABLE scraped_articles_raw (
    scrape_id SERIAL PRIMARY KEY,
    page_path VARCHAR(1024) NOT NULL,
    url VARCHAR(2048) NOT NULL,
    title VARCHAR(500),
    subtitle TEXT,
    author VARCHAR(255),
    categoria VARCHAR(100),
    publication_date DATE,
    published_text VARCHAR(100),
    body_html TEXT,
    body_text TEXT,
    archive_scraped_at TIMESTAMP WITH TIME ZONE,
    detail_scraped_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

### Query Utili

```sql
-- Conteggio articoli per categoria
SELECT categoria, COUNT(*) as count
FROM latest_scraped_articles
GROUP BY categoria
ORDER BY count DESC;

-- Articoli recenti
SELECT title, author, publication_date, created_at
FROM latest_scraped_articles
ORDER BY publication_date DESC
LIMIT 10;

-- Statistiche scraping
SELECT 
    COUNT(*) as total_articles,
    COUNT(DISTINCT author) as unique_authors,
    COUNT(DISTINCT categoria) as unique_categories,
    MAX(created_at) as last_scrape
FROM scraped_articles_raw;
```

---

## 🎯 Best Practices

### 1. Rate Limiting

Il sistema implementa rate limiting automatico:
- **Delay tra richieste**: 2 secondi (default)
- **Batch processing**: 100 articoli per batch
- **Pausa tra batch**: 2 minuti

**Non ridurre** questi valori per rispetto del sito!

### 2. Error Handling

Il sistema gestisce automaticamente:
- Errori di rete (3 retry automatici)
- Timeout richieste
- Parsing HTML fallito
- Errori database

Tutti gli errori sono loggati in dettaglio.

### 3. Logging

I log sono salvati in:
- Console: output real-time con colori
- File: `logs/scraping_scheduler_{timestamp}.log`
- JSON: `logs/scraping_results_{timestamp}.json`

Retention automatica: 3 mesi.

### 4. Monitoring

Usa il pattern Observer per monitoraggio custom:

```python
from etl.articles_db_pipeline.scrapers import ScraperObserver

class CustomObserver(ScraperObserver):
    def on_progress_update(self, progress):
        # Invia notifica, aggiorna UI, etc.
        print(f"Progress: {progress.progress_percentage:.1f}%")
    
    def on_batch_complete(self, batch_num, batch_size):
        # Log batch completion
        print(f"Batch {batch_num} done!")
    
    def on_error(self, error, url):
        # Alert su errori
        send_alert(f"Error at {url}: {error}")

# Aggiungi observer
pipeline = WebScrapingPipeline()
pipeline.extractor.add_observer(CustomObserver())
```

---

## 🔧 Troubleshooting

### Problema: "Connection refused"

**Causa:** Database non in esecuzione

**Soluzione:**
```bash
cd articles_db
docker-compose up -d
```

### Problema: "Too many requests" / 429 errors

**Causa:** Rate limiting troppo aggressivo

**Soluzione:** Aumenta delay:
```bash
python web_scraping_cli.py scrape --delay 5.0 --batch-pause 300
```

### Problema: Selectors non trovano elementi

**Causa:** Layout sito cambiato

**Soluzione:** 
1. Ispeziona pagina con browser dev tools
2. Aggiorna selectors in `scrapers/selectors.py`
3. Ri-esegui scraping

### Problema: Scheduler non parte

**Causa:** Dipendenza `schedule` mancante

**Soluzione:**
```bash
pip install schedule
```

### Problema: Errori parsing date

**Causa:** Formato data cambiato

**Soluzione:** Check logs, aggiorna parsing in `article_detail_scraper.py`

---

## 📊 Metriche e Performance

### Performance Tipiche

Con impostazioni default:
- **Velocità**: ~30 articoli/minuto
- **Tempo totale** (1000 articoli): ~40 minuti
- **Memoria**: <500 MB
- **Spazio DB**: ~5 MB per 1000 articoli

### Ottimizzazioni Possibili

⚠️ **Da valutare con cautela!**

```python
# Più veloce (ma meno rispettoso)
pipeline = WebScrapingPipeline(
    delay_between_requests=1.0,    # Da 2.0 → 1.0
    batch_size=150,                # Da 100 → 150
    batch_pause_duration=60        # Da 120 → 60
)
```

---

## 📝 Note Aggiuntive

### Manutenzione

- **Backup database**: Settimanale automatico (vedi `articles_db/scripts`)
- **Log rotation**: Automatica (3 mesi retention)
- **Monitoring**: Via Observer pattern + log analysis

### Estensibilità

Il sistema è progettato per essere esteso:

1. **Nuovi selectors**: Aggiungi a `selectors.py`
2. **Nuovi campi**: Aggiorna modelli in `models/scraped_article.py`
3. **Nuovi observers**: Implementa `ScraperObserver`
4. **Post-processing**: Aggiungi transformers nella pipeline

### Sicurezza

- ✅ Parametrized SQL queries (no SQL injection)
- ✅ Input validation con Pydantic
- ✅ Rate limiting rispettoso
- ✅ User agent identificabile
- ✅ Logs sanitizzati (no credenziali)

---

## 📞 Supporto

Per problemi o domande:
1. Check questa documentazione
2. Controlla logs in `logs/`
3. Esegui diagnostics: `python web_scraping_cli.py status`
4. Review codice sorgente (ben commentato!)

---

**Buon scraping! 🚕📰**
