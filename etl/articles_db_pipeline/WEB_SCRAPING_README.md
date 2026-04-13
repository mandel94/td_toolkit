# 🕷️ Web Scraping Module

**Modulo professionale per lo scraping automatico degli articoli da TaxiDrivers.it**

## 🎯 Cosa Fa

Questo modulo estrae automaticamente gli articoli dal sito TaxiDrivers.it e li salva in un database PostgreSQL per analisi future. Include:

- ✅ **Scraping completo** di archivio e dettagli articoli
- ✅ **Batch processing** intelligente (100 articoli, pausa 2 min)
- ✅ **Observer pattern** per monitoraggio real-time
- ✅ **Scheduling automatico** (esecuzione settimanale)
- ✅ **Database integration** con modello dimensionale
- ✅ **Error handling** robusto con retry logic

## 📋 Quick Start

### 1. Installazione

```bash
cd etl/articles_db_pipeline
pip install -r requirements.txt
```

### 2. Avvia Database

```bash
cd ../../articles_db
docker-compose up -d
```

### 3. Esegui Scraping

**Windows:**
```powershell
cd etl/articles_db_pipeline
python web_scraping_cli.py scrape --limit 20  # Test con 20 articoli
```

**Linux/Mac:**
```bash
cd etl/articles_db_pipeline
python web_scraping_cli.py scrape --limit 20
```

## 🚀 Utilizzo

### Comandi Principali

```bash
# Scraping completo
python web_scraping_cli.py scrape

# Verifica stato database
python web_scraping_cli.py status

# Aggiorna tabelle dimensionali
python web_scraping_cli.py update-dim

# Aiuto
python web_scraping_cli.py --help
```

### Scheduling Automatico

**Windows:**
```powershell
.\Start-WebScraping-Scheduler.ps1
```

**Linux/Mac:**
```bash
python scheduler.py
```

Default: **Ogni venerdì alle 02:00**

## 📊 Dati Estratti

### Dalla Pagina Archivio
- Titolo articolo
- Categoria
- URL articolo
- Testo pubblicazione

### Dalla Pagina Dettaglio
- Sottotitolo
- Autore
- Data pubblicazione (formato ISO)
- Corpo HTML completo
- Corpo testo plain

## 🏗️ Architettura

```
etl/articles_db_pipeline/
├── scrapers/                    # Modulo scraping riusabile
│   ├── base_scraper.py         # Base class con Observer
│   ├── archive_scraper.py      # Scraper archivio
│   ├── article_detail_scraper.py # Scraper dettagli
│   └── selectors.py            # CSS selectors centralizzati
├── extractors/
│   └── web_scraping_extractor.py
├── loaders/
│   └── scraped_articles_loader.py
├── models/
│   └── scraped_article.py
├── web_scraping_pipeline.py    # Pipeline principale
├── scheduler.py                # Scheduler settimanale
└── web_scraping_cli.py         # CLI
```

## 📖 Documentazione

- **[WEB_SCRAPING_GUIDE.md](WEB_SCRAPING_GUIDE.md)** - Guida completa (⭐ LEGGI PRIMA)
- **[QUICK_REFERENCE.md](QUICK_REFERENCE.md)** - Riferimento rapido
- **[README.md](README.md)** - Questo file

## ⚙️ Configurazione

### Parametri Default

```python
delay_between_requests = 2.0      # Secondi tra richieste
batch_size = 100                  # Articoli per batch
batch_pause_duration = 120        # Pausa tra batch (secondi)
timeout = 30                      # Timeout richieste
max_retries = 3                   # Tentativi retry
```

### Personalizzazione

```bash
python web_scraping_cli.py scrape \
  --delay 3.0 \
  --batch-size 50 \
  --batch-pause 180
```

## 💾 Database

### Tabella Principale

```sql
scraped_articles_raw (
    scrape_id, page_path, url, title, subtitle,
    author, categoria, publication_date,
    body_html, body_text, ...
)
```

### View

```sql
latest_scraped_articles  -- Ultima versione di ogni articolo
```

### Query Esempio

```sql
-- Ultimi 10 articoli
SELECT title, author, publication_date
FROM latest_scraped_articles
ORDER BY publication_date DESC
LIMIT 10;

-- Statistiche per categoria
SELECT categoria, COUNT(*) as total
FROM latest_scraped_articles
GROUP BY categoria
ORDER BY total DESC;
```

## 🔧 Troubleshooting

| Problema | Soluzione |
|----------|-----------|
| Database connection error | Avvia database con `docker-compose up -d` |
| Rate limiting (429) | Aumenta `--delay` e `--batch-pause` |
| Selectors non funzionano | Aggiorna `scrapers/selectors.py` |
| Dipendenze mancanti | `pip install -r requirements.txt` |

## 📈 Performance

- **Velocità**: ~30 articoli/minuto
- **Tempo** (1000 articoli): ~40 minuti
- **Memoria**: <500 MB
- **Spazio DB**: ~5 MB / 1000 articoli

## 🎯 Best Practices

1. ✅ **Rispetta rate limits** - Non ridurre delay/pause
2. ✅ **Monitora log** - Check `logs/` directory
3. ✅ **Test prima** - Usa `--limit` per test
4. ✅ **Backup database** - Automatico settimanale
5. ✅ **Update selectors** - Se layout sito cambia

## 🔐 Sicurezza

- ✅ Parametrized SQL queries
- ✅ Input validation con Pydantic
- ✅ Rate limiting rispettoso
- ✅ User agent identificabile
- ✅ No credenziali nei log

## 🚦 Status Check

Verifica sistema funzionante:

```bash
python web_scraping_cli.py status
```

Output atteso:
```
Database connected: True
Total scraped articles: X
Latest scrape: YYYY-MM-DD HH:MM:SS
```

## 📞 Supporto

1. Leggi **WEB_SCRAPING_GUIDE.md** (documentazione completa)
2. Controlla **logs/** per errori
3. Esegui `python web_scraping_cli.py status`
4. Review codice (ben commentato!)

## 🎓 Pattern & Principi

- **Observer Pattern** - Monitoraggio real-time
- **Batch Processing** - Gestione memoria efficiente
- **Retry Logic** - Resilienza agli errori
- **Centralized Config** - Manutenibilità
- **Type Safety** - Pydantic models
- **Logging** - Tracciabilità completa

---

## 📝 Esempi Pratici

### Scenario 1: Test Rapido

```bash
# Testa con 10 articoli
python web_scraping_cli.py scrape --limit 10 -v
```

### Scenario 2: Scraping Completo

```bash
# Tutti gli articoli + salva risultati
python web_scraping_cli.py scrape --output results.json
```

### Scenario 3: Scheduling Produzione

```powershell
# Windows - Esegui ogni venerdì alle 2:00
.\Start-WebScraping-Scheduler.ps1
```

### Scenario 4: Programmatic Usage

```python
from etl.articles_db_pipeline.web_scraping_pipeline import WebScrapingPipeline

# Crea e esegui pipeline
pipeline = WebScrapingPipeline()
results = pipeline.run_full_pipeline(limit=50)

print(f"Success: {results['success']}")
print(f"Articles: {results['extraction']['successful']}")
```

---

**🚕 Happy Scraping! 📰**

Per la documentazione completa: [WEB_SCRAPING_GUIDE.md](WEB_SCRAPING_GUIDE.md)
