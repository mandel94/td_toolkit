# Content Scoring System

Sistema modulare per il calcolo del **Content Score** di articoli editoriali basato su metriche di engagement e traffico.

## 🎯 Panoramica

Il Content Scoring System implementa best practices di data engineering per trasformare metriche grezze di Google Analytics in un indicatore sintetico normalizzato (0-100) che riflette il valore editoriale di ogni articolo.

### Caratteristiche Principali

- ✅ **Modularità**: Componenti separati e riutilizzabili
- ✅ **Idempotenza**: Risultati consistenti e riproducibili
- ✅ **Flessibilità**: Supporto per multiple sorgenti di dati
- ✅ **Configurabilità**: Pesi e parametri facilmente modificabili
- ✅ **Validazione**: Anomaly detection e quality checks integrati
- ✅ **Segmentazione**: Categorizzazione automatica degli articoli

## 📁 Struttura

```
etl/content_scoring/
├── __init__.py           # Package initialization
├── config.py             # Configurazione centralizzata
├── calculator.py         # Logica di calcolo score
├── segmentation.py       # Classificazione articoli
├── validators.py         # Validazione e anomaly detection
└── README.md            # Questa documentazione
```

## 🚀 Quick Start

### Utilizzo Base

```python
from etl.content_scoring import ContentScoreCalculator

# Inizializza il calculator
calculator = ContentScoreCalculator()

# Calcola gli score su un DataFrame
scored_df = calculator.calculate(df)

# Il DataFrame ora include:
# - content_score: Score finale 0-100
# - reach_score: Componente reach
# - loyalty_score: Componente loyalty
# - efficiency_score: Componente efficiency
```

### Con Segmentazione

```python
from etl.content_scoring import ContentScoreCalculator, ContentScoreSegmentation

calculator = ContentScoreCalculator()
segmenter = ContentScoreSegmentation()

# Calcola e segmenta
scored_df = calculator.calculate(df)
segmented_df = segmenter.segment(scored_df)

# Stampa distribuzione segmenti
print(segmented_df['content_segment'].value_counts())
```

### Con Validazione

```python
from etl.content_scoring import ContentScoreValidator

validator = ContentScoreValidator()

# Valida i dati
is_valid, issues = validator.validate(scored_df)

if not is_valid:
    for issue in issues:
        print(f"{issue['type']}: {issue['message']}")

# Aggiungi flag per anomalie
flagged_df = validator.flag_anomalies(scored_df)
```

## ⚙️ Configurazione

### Configurazione Standard

Il sistema usa una configurazione predefinita che può essere modificata:

```python
from etl.content_scoring import ContentScoreCalculator, ContentScoringConfig

# Crea una configurazione personalizzata
config = ContentScoringConfig(
    reach_weight=0.25,          # 25% peso reach
    loyalty_weight=0.50,        # 50% peso loyalty
    efficiency_weight=0.25,     # 25% peso efficiency
    
    min_views_threshold=20,     # Minimo 20 views per score valido
    log_transform_views=True,   # Applica log scaling alle views
    
    top_performer_threshold=85.0,     # Soglia per "Top Performer"
    high_engagement_threshold=0.60,   # Soglia engagement rate
)

calculator = ContentScoreCalculator(config=config)
```

### Mapping Metriche Personalizzato

Se le tue colonne hanno nomi diversi:

```python
config = ContentScoringConfig(
    metrics_mapping={
        'views': 'pageviews',              # Nome colonna views
        'engagement_rate': 'engagement',    # Nome colonna engagement
        'bounce_rate': 'bounce',           # Nome colonna bounce
        'session_duration': 'duration'     # Nome colonna duration
    }
)
```

## 📊 Formula del Content Score

Il Content Score è calcolato come media ponderata di tre componenti:

```
Content Score = (Reach × 0.30) + (Loyalty × 0.40) + (Efficiency × 0.30)
```

### Componenti

1. **Reach Score (30%)**
   - Basato su: `screenPageViews`
   - Trasformazione: Log scaling per gestire outliers
   - Normalizzazione: Min-Max [0, 100]

2. **Loyalty Score (40%)**
   - Basato su: `engagementRate` + `averageSessionDuration`
   - Media delle due metriche normalizzate
   - Peso maggiore perché riflette qualità del contenuto

3. **Efficiency Score (30%)**
   - Basato su: `engagementRate` (già l'opposto del bounce rate)
   - Normalizzazione: Min-Max [0, 100]
   - **Nota**: engagementRate è già una metrica positiva (opposto di bounceRate)

## 🏷️ Segmentazione Articoli

Il sistema classifica automaticamente gli articoli in 5 segmenti:

### 1. Top Performer
- **Criterio**: Score ≥ 80
- **Strategia**: Amplify and Replicate
- **Azioni**: 
  - Promuovere su tutti i canali
  - Analizzare pattern per replicare
  - Creare contenuti correlati

### 2. Niche Value
- **Criterio**: Alto engagement (≥50%) ma basso traffico (<100 views)
- **Strategia**: Expand Reach
- **Azioni**:
  - Aumentare budget promozionale
  - Ottimizzare SEO
  - Campagne social mirate

### 3. Rising Star
- **Criterio**: Score ≥ 60, engagement ≥30%, traffico moderato
- **Strategia**: Nurture Growth
- **Azioni**:
  - Supporto editoriale incrementale
  - Serie di contenuti correlati
  - Monitoraggio per promozione

### 4. Underperforming
- **Criterio**: Alto traffico ma score <40 e (bounce >70% o engagement <20%)
- **Strategia**: Optimize or Redirect
- **Azioni**:
  - Migliorare qualità contenuto
  - Ottimizzare UX/performance
  - Content refresh

### 5. Standard
- **Criterio**: Tutti gli altri
- **Strategia**: Maintain and Monitor
- **Azioni**:
  - Continuare cadenza regolare
  - Cercare opportunità di miglioramento

## 🔍 Validazione e Anomaly Detection

Il sistema include controlli automatici per garantire la qualità dei dati:

### Controlli Implementati

1. **Required Columns**: Verifica presenza colonne necessarie
2. **Data Types**: Validazione tipi di dato
3. **Value Ranges**: Controllo valori fuori range
4. **Statistical Anomalies**: Rilevamento outliers con IQR method
5. **Score Consistency**: Identifica score illogici
6. **Significance**: Segnala score con dati insufficienti

### Esempio Output Validazione

```python
is_valid, issues = validator.validate(df)

# issues può contenere:
[
    {
        'type': 'LowSignificance',
        'severity': 'INFO',
        'count': 5,
        'message': '5 articles have high scores but fewer than 10 views'
    },
    {
        'type': 'StatisticalOutlier',
        'severity': 'INFO',
        'count': 3,
        'percentage': 3.0,
        'message': '3 (3.0%) scores are statistical outliers'
    }
]
```

## 📈 Utilizzo Avanzato

### Input da File

```python
# Da CSV
scored_df = calculator.calculate('data/articles.csv')

# Da Excel
scored_df = calculator.calculate('data/articles.xlsx')
```

### Batch Processing

```python
# Calcola score per multipli dataset
results = calculator.calculate_batch([
    'week1.csv',
    'week2.csv',
    'week3.csv'
])
```

### Report Completo Segmentazione

```python
from etl.content_scoring import ContentScoreSegmentation

segmenter = ContentScoreSegmentation()

# Genera report completo con raccomandazioni
report = segmenter.create_segment_report(
    segmented_df,
    output_path='segment_report.json'
)

# report contiene:
# - summary: Statistiche generali
# - statistics: Metriche per segmento
# - recommendations: Azioni consigliate per segmento
```

### Statistiche per Segmento

```python
stats = segmenter.get_segment_statistics(segmented_df)
print(stats)

#    content_segment  screenPageViews_count  screenPageViews_mean  ...
# 0  Top Performer                       15               1234.5  ...
# 1  Rising Star                         25                567.8  ...
# 2  Standard                            45                234.2  ...
```

## 🔧 Integrazione con Report Esistenti

Il sistema è già integrato nel `sandra_report.py`:

```python
# Nel sandra_report.py
from etl.content_scoring import (
    ContentScoreCalculator,
    ContentScoreSegmentation,
    ContentScoreValidator
)

# Inizializza componenti
calculator = ContentScoreCalculator()
segmenter = ContentScoreSegmentation()
validator = ContentScoreValidator()

# Calcola scores
top_df = calculator.calculate(top_df)
top_df = segmenter.segment(top_df)

# Valida risultati
is_valid, issues = validator.validate(top_df)
top_df = validator.flag_anomalies(top_df)
```

## 📝 Note Tecniche

### Requisiti

- pandas >= 1.3.0
- numpy >= 1.20.0
- Python >= 3.8

### Performance

- Calcolo score: ~1000 articoli/secondo
- Normalizzazione: O(n) complessità
- Validazione: O(n) complessità

### Idempotenza

Eseguire il calcolo più volte sugli stessi dati produce **esattamente** gli stessi risultati. Questo è garantito da:
- Nessun uso di randomness
- Trasformazioni deterministiche
- Configurazione immutabile

### Estensibilità

Per aggiungere nuove metriche o componenti:

1. Aggiorna `ContentScoringConfig.metrics_mapping`
2. Modifica `_calculate_component_scores()` in `calculator.py`
3. Aggiusta i pesi per mantenere somma = 1.0

## 🐛 Troubleshooting

### "Missing required columns" Error

**Problema**: Colonne GA4 hanno nomi diversi
**Soluzione**: Usa `metrics_mapping` nella configurazione

```python
config = ContentScoringConfig(
    metrics_mapping={
        'views': 'il_tuo_nome_colonna_views',
        # ...
    }
)
```

### Score sempre 100 o 0

**Problema**: Tutti i valori sono uguali (nessuna varianza)
**Soluzione**: Verifica che i dati abbiano effettiva variabilità

### "Weights must sum to 1.0" Error

**Problema**: Pesi non sommano a 1.0
**Soluzione**: Controlla che `reach_weight + loyalty_weight + efficiency_weight = 1.0`

## 📚 Riferimenti

- [Google Analytics 4 Data API](https://developers.google.com/analytics/devguides/reporting/data/v1)
- [Content Score Brief](../Context%20AI/articles_content_scoring_brief.md)
- Design basato su best practices 2025 per data engineering

## 👥 Autori

Havas Analytics Team - 2025

## 📄 Licenza

Uso interno Havas
