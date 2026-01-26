# Taxi Drivers Website Performance Analysis System

Un sistema completo per l'analisi delle performance del sito web Taxi Drivers, con confronti storici automatici, esportazione JSON strutturata e generazione di report PDF professionali.

## 🚀 Caratteristiche Principali

### ✅ **Analisi Completa delle Metriche GA4**
- Visualizzazioni pagine, utenti attivi, nuovi utenti
- Sessioni, bounce rate, engagement rate
- Tempo medio di sessione e conversioni
- Breakdown per dispositivo, canale e geografia

### ✅ **Confronti Storici Automatici**  
- Calcolo automatico di tutti i periodi precedenti della stessa durata
- Limite configurabile a gennaio 2025
- Analisi dei trend e identificazione di pattern
- Confronto intelligente tra periodi equivalenti

### ✅ **Esportazione Dati JSON**
- Struttura JSON completa con tutti i dati estratti
- Metadati comprensivi per ogni periodo analizzato
- Hash di integrità dati per validazione
- Report di qualità dei dati integrato

### ✅ **Report PDF Professionale**
- Executive summary con insight chiave
- Analisi comparativa tra periodi recenti
- Grafici e visualizzazioni professionali
- Layout corporate con branding Taxi Drivers

### ✅ **Best Practices Ingegneria Dati**
- Logging completo e configurabile
- Validazione robusta degli input
- Gestione errori dettagliata
- Separazione delle responsabilità (SoC)
- Type hints per maintainability
- Documentazione completa

## 📁 Struttura del Modulo

```
performance_analysis/
├── __init__.py                  # Modulo principale
├── performance_report.py        # Script CLI principale  
├── metrics_extractor.py         # Estrazione metriche GA4
├── periods_manager.py          # Gestione periodi storici
├── json_exporter.py            # Esportazione JSON
├── pdf_generator.py            # Generazione report PDF
├── requirements.txt            # Dipendenze
└── README.md                   # Questa documentazione
```

## 🛠️ Installazione

### Prerequisiti
- Python 3.8+
- Accesso all'API GA4 configurato (disponibile nel progetto parent)
- Dipendenze del progetto Taxi Drivers

### Installazione Dipendenze
```bash
# Dalla directory performance_analysis
pip install -r requirements.txt
```

## 💻 Utilizzo

### Comando Base
```bash
python performance_report.py --start-date 2025-11-01 --end-date 2025-11-07
```

### Esempi Completi

#### Report Settimanale
```bash
python performance_report.py \
    --start-date 2025-11-01 \
    --end-date 2025-11-07 \
    --title "Weekly Performance Report"
```

#### Report Mensile con Storia Limitata
```bash  
python performance_report.py \
    --start-date 2025-11-01 \
    --end-date 2025-11-30 \
    --max-periods 12 \
    --output-dir ./monthly_reports
```

#### Solo Esportazione JSON (per automazioni)
```bash
python performance_report.py \
    --start-date 2025-11-01 \
    --end-date 2025-11-07 \
    --no-pdf \
    --quiet
```

#### Report Custom con Configurazioni Avanzate
```bash
python performance_report.py \
    --start-date 2025-10-01 \
    --end-date 2025-10-31 \
    --max-periods 24 \
    --property-id 394327334 \
    --min-date 2025-01-01 \
    --title "Q4 Performance Analysis" \
    --verbose
```

### Parametri CLI

| Parametro | Tipo | Descrizione | Default |
|-----------|------|-------------|---------|
| `--start-date` | **Required** | Data inizio analisi (YYYY-MM-DD) | - |
| `--end-date` | **Required** | Data fine analisi (YYYY-MM-DD) | - |
| `--output-dir` | Optional | Directory output files | `output` |
| `--max-periods` | Optional | Max periodi storici da analizzare | Tutti disponibili |
| `--property-id` | Optional | ID proprietà GA4 | `394327334` |
| `--min-date` | Optional | Data minima per analisi storica | `2025-01-01` |
| `--title` | Optional | Titolo del report | `Taxi Drivers Website Performance Report` |
| `--no-pdf` | Flag | Salta generazione PDF | False |
| `--no-json` | Flag | Salta esportazione JSON | False |
| `--verbose` | Flag | Logging dettagliato | False |
| `--quiet` | Flag | Solo errori in output | False |

## 📊 Output Generati

### 1. **File JSON** (`performance_analysis_YYYYMMDD_YYYYMMDD.json`)
Struttura completa:
```json
{
  "schema_version": "1.0.0",
  "export_metadata": {
    "export_timestamp": "2025-11-24T10:30:00Z",
    "total_periods": 10,
    "analysis_range": {...}
  },
  "analysis_configuration": {...},
  "summary_statistics": {...},
  "periods_data": [...],
  "data_quality_report": {...}
}
```

### 2. **Report PDF** (`performance_report_YYYYMMDD_YYYYMMDD.pdf`)
Sezioni incluse:
- **Title Page**: Highlights chiave e panoramica
- **Executive Summary**: Findings principali e insight
- **Metrics Comparison**: Tabelle comparative dettagliate  
- **Performance Trends**: Analisi dei trend storici
- **Detailed Breakdown**: Metriche per categoria
- **Charts & Visualizations**: Grafici professionali

### 3. **Summary Report** (`performance_summary_YYYYMMDD_HHMMSS.txt`)
Riassunto testuale con:
- Metriche chiave del periodo corrente
- Confronti period-over-period
- Lista dei file generati

## 🏗️ Architettura Tecnica

### Componenti Principali

#### `PerformanceMetricsExtractor`
- **Responsabilità**: Estrazione metriche da GA4
- **Input**: Date, configurazione metriche
- **Output**: Dati strutturati con qualità assessment

#### `HistoricalPeriodsManager` 
- **Responsabilità**: Calcolo periodi storici
- **Features**: Validazione, allineamento, classificazione automatica
- **Smart Logic**: Gestione periodi custom, settimanali, mensili

#### `PerformanceDataExporter`
- **Responsabilità**: Esportazione JSON strutturata
- **Features**: Schema validation, integrity hashing, metadata completi

#### `PDFReportGenerator`
- **Responsabilità**: Report PDF professionali
- **Features**: Charts matplotlib, layout corporate, analisi comparative

#### `PerformanceReportOrchestrator`
- **Responsabilità**: Coordinamento componenti
- **Features**: Error handling, logging, validation input

### Flusso Dati

```
Input Dates → Periods Calculation → GA4 Data Extraction → 
Analysis & Processing → JSON Export + PDF Generation → Output Files
```

### Error Handling
- **Validation robusta** degli input
- **Graceful degradation** per dati mancanti  
- **Logging dettagliato** per debugging
- **Retry logic** per chiamate API GA4
- **Data quality assessment** automatico

## 🔧 Configurazione Avanzata

### Variabili d'Ambiente
```bash
# Logging level (DEBUG, INFO, WARNING, ERROR)
export PERFORMANCE_LOG_LEVEL=INFO

# Output directory default
export PERFORMANCE_OUTPUT_DIR=./reports

# GA4 API timeout (seconds)
export GA4_API_TIMEOUT=30
```

### Personalizzazioni

#### Metriche Custom
Modifica `metrics_extractor.py` per aggiungere metriche specifiche:
```python
self.key_metrics = [
    'screenPageViews',
    'activeUsers', 
    # Aggiungi metriche custom qui
    'customMetric1',
    'customMetric2'
]
```

#### Styling PDF
Modifica colori corporate in `pdf_generator.py`:
```python
self.colors = {
    'primary': HexColor('#YOUR_PRIMARY_COLOR'),
    'secondary': HexColor('#YOUR_SECONDARY_COLOR'),
    # ...
}
```

## 🧪 Testing

### Test Unitari
```bash
# Dalla directory performance_analysis
python -m pytest tests/ -v
```

### Test di Integrazione
```bash
# Test con dati reali (richiede API GA4 configurata)
python performance_report.py --start-date 2025-11-20 --end-date 2025-11-21 --verbose
```

## 📈 Esempi di Output

### Metriche Tipiche Estratte
- **Page Views**: 50,000+ visualizzazioni settimanali
- **Active Users**: 15,000+ utenti attivi
- **New Users**: 8,000+ nuovi utenti  
- **Sessions**: 25,000+ sessioni
- **Bounce Rate**: ~65% bounce rate medio
- **Engagement Rate**: ~35% engagement rate

### Insights Automatici
- Identificazione trend crescenti/decrescenti
- Confronto performance period-over-period
- Analysis seasonal patterns
- Quality assessment automatico

## 🐛 Troubleshooting

### Problemi Comuni

#### Errore: "No data returned from GA4"
- Verifica configurazione API GA4
- Controlla property ID
- Verifica date range (non troppo recente)

#### Errore: "Insufficient periods for comparison"
- Aumenta range date o riduci `max-periods`
- Verifica `min-date` setting

#### PDF Generation Failed
- Verifica installazione `reportlab`
- Controlla permessi directory output
- Verifica disponibilità memoria per grafici

### Logging e Debug
```bash
# Logging completo
python performance_report.py --verbose --start-date ... --end-date ...

# Solo errori  
python performance_report.py --quiet --start-date ... --end-date ...

# Log file location
tail -f performance_analysis.log
```

## 🤝 Contributi

Per miglioramenti e bug fixes:
1. Seguire le best practices del progetto Taxi Drivers
2. Mantenere documentazione aggiornata
3. Includere test per nuove features
4. Rispettare convenzioni naming esistenti

## 📝 Changelog

### v1.0.0 (2025-11-24)
- ✅ Release iniziale
- ✅ Estrazione metriche GA4 completa
- ✅ Sistema periodi storici automatico
- ✅ Esportazione JSON strutturata  
- ✅ Generazione report PDF professionale
- ✅ CLI completo con validazione
- ✅ Documentazione completa

---

*Sistema sviluppato seguendo le best practices di ingegneria dei dati per garantire affidabilità, maintainability e scalabilità.*