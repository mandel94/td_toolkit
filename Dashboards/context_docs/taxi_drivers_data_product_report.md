# 📊 Data Product Report  
## Taxi Drivers – Visualizations Trend Explorer

**Fonte dati:** Google Analytics 4  
**Utenti target:** Team editoriale / direzione contenuti  
**Obiettivo primario di analisi:**  
✔️ Qual è il trend delle visualizzazioni nel periodo?

---

## 1. Obiettivo di analisi  
*(Problem framing – best practices 2025)*

### Domanda chiave
> Il sito *Taxi Drivers* sta crescendo, stagnando o perdendo attenzione nel tempo?

### Traduzione in metriche
- **Page Views (`screenPageViews`)** come proxy di interesse editoriale
- Analisi **temporale**, non focalizzata sul singolo articolo (fase 1 del data product)

### Rilevanza per il team editoriale
- Valutare se le scelte editoriali producono **crescita di attenzione nel medio periodo**
- Distinguere:
  - fluttuazioni fisiologiche
  - stagionalità
  - cambi di trend strutturali

---

## 2. Scope del data product

### In scope
- Trend delle visualizzazioni
- Confronti temporali
- Stagionalità
- Evidenziazione di cambiamenti rilevanti nel tempo

### Out of scope (fase iniziale)
- Analisi SEO avanzata
- Performance per singolo articolo o autore
- Metriche di conversione o revenue

> Approccio incrementale: il data product cresce per **layer successivi**, non tutto subito.

---

## 3. Fonte dati: Google Analytics 4

### Metriche GA4 utilizzate

| Metrica | Motivazione |
|-------|------------|
| `screenPageViews` | metrica primaria di consumo |
| `date` | asse temporale |
| `activeUsers` (opzionale) | contesto di lettura |

### Granularità
- Giornaliera come base
- Aggregazione dinamica (settimanale / mensile) in dashboard

---

## 4. Feature engineering  
*(Editor-friendly, non data-centric)*

### Feature temporali
- `date`
- `week`
- `month`
- `year`
- `weekday`

### Feature analitiche

| Feature | Significato editoriale |
|------|-----------------------|
| `views_7d_avg` | trend smussato di breve periodo |
| `views_30d_avg` | trend editoriale di medio periodo |
| `pct_change_wow` | variazione settimana su settimana |
| `trend_direction` | ↑ crescita / ↓ calo / → stabilità |

---

## 5. Logica di analisi  
*(Insight-first, AI-ready)*

### 5.1 Trend primario
- Line chart delle visualizzazioni nel tempo
- Media mobile per ridurre il rumore giornaliero

### 5.2 Stagionalità
- Pattern settimanali (giorni forti/deboli)
- Pattern mensili

### 5.3 Cambi di regime
- Periodi di crescita sostenuta
- Periodi di calo strutturale

> **Best practice 2025:**  
> non mostrare tutte le metriche → guidare l’interpretazione.

---

## 6. Architettura del data product

```text
Google Analytics 4 API
        ↓
ETL Python (scheduled)
        ↓
DataFrame pulito (pandas)
        ↓
Dash App (dashboard editoriale)
