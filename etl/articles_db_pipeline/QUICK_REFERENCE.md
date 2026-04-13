# Quick Reference Guide - Web Scraping Module

## 🚀 Comandi Rapidi

### Esecuzione Manuale

```bash
# Scraping completo
python web_scraping_cli.py scrape

# Test con 20 articoli
python web_scraping_cli.py scrape --limit 20

# Verifica stato
python web_scraping_cli.py status

# Aggiorna solo dimensioni
python web_scraping_cli.py update-dim
```

### Scheduler (Windows)

```powershell
# Avvia scheduler (venerdì 02:00)
.\Start-WebScraping-Scheduler.ps1

# Test immediato
.\Start-WebScraping-Scheduler.ps1 -RunNow

# Personalizza schedule
.\Start-WebScraping-Scheduler.ps1 -Day monday -Time 03:30
```

### Scheduler (Linux/Mac)

```bash
# Avvia scheduler
python scheduler.py

# Test immediato
python scheduler.py --run-now

# Personalizza
python scheduler.py --day monday --time 03:30
```

## 📊 Dati Estratti

### Pagina Archivio
- ✅ Titolo articolo
- ✅ Categoria
- ✅ URL articolo

### Pagina Dettaglio Articolo
- ✅ Sottotitolo
- ✅ Autore
- ✅ Data pubblicazione
- ✅ Corpo HTML completo
- ✅ Corpo testo plain

## 🎯 Parametri Chiave

| Parametro | Valore Default | Descrizione |
|-----------|----------------|-------------|
| `delay_between_requests` | 2.0 sec | Pausa tra richieste HTTP |
| `batch_size` | 100 | Articoli per batch |
| `batch_pause_duration` | 120 sec | Pausa tra batch (2 min) |
| `schedule_day` | "friday" | Giorno esecuzione |
| `schedule_time` | "02:00" | Ora esecuzione |

## 📁 Output

### Database
- Tabella: `scraped_articles_raw`
- View: `latest_scraped_articles`
- Dimensioni aggiornate: `dim_articles`, `dim_authors`, `dim_categories`

### Log Files
- Location: `etl/articles_db_pipeline/logs/`
- Formato: `scraping_scheduler_{timestamp}.log`
- Risultati: `scraping_results_{timestamp}.json`

## 🔧 Troubleshooting

| Problema | Soluzione |
|----------|-----------|
| Database error | `docker-compose up -d` in articles_db/ |
| Rate limiting | Aumenta `--delay` e `--batch-pause` |
| Selector errors | Aggiorna `scrapers/selectors.py` |
| Missing packages | `pip install -r requirements.txt` |

## 📖 Documentazione Completa

Vedi: `WEB_SCRAPING_GUIDE.md`

## 🏗️ Architettura

```
Archive Page → List Articles → Article Details → Database
   (1)              (2)             (3)            (4)
                              
        [Batch Processing: 100 items, 2 min pause]
        [Observer Pattern: Real-time monitoring]
```

## ✅ Checklist Pre-Esecuzione

- [ ] Database in esecuzione
- [ ] Dipendenze installate (`pip install -r requirements.txt`)
- [ ] Connettività internet attiva
- [ ] Spazio disco sufficiente (5MB per 1000 articoli)

---

**Per informazioni dettagliate, consulta WEB_SCRAPING_GUIDE.md**
