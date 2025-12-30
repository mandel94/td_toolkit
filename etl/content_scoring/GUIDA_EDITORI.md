# Guida all'Editorial Score
## Sistema di Valutazione dei Contenuti - Taxi Drivers

**Versione 1.0 - Dicembre 2025**  
*Documento per uso interno del team editoriale*

---

## 📖 Cos'è l'Editorial Score?

L'**Editorial Score** è un punteggio da **0 a 100** che misura la **forza editoriale complessiva** di ogni articolo pubblicato su Taxi Drivers, combinando tre dimensioni fondamentali della performance:

- 🎯 **Reach** (Portata): Quante persone raggiunge l'articolo
- ❤️ **Engagement** (Coinvolgimento): Quanto gli utenti si interessano al contenuto
- ⏱️ **Depth** (Profondità): Quanto tempo gli utenti dedicano alla lettura

Il punteggio serve a **confrontare gli articoli tra loro** e identificare quali contenuti funzionano meglio, indipendentemente dal periodo di pubblicazione o dal volume di traffico assoluto.

---

## 🎯 Come si Calcola lo Score?

### Le Tre Componenti

L'Editorial Score si basa su tre metriche che **non si sovrappongono**, per evitare di contare due volte lo stesso tipo di performance:

#### 1. **Reach** (35% del punteggio finale)
**Cosa misura**: La capacità di attrarre pubblico

**Fonte dati**: Numero di visualizzazioni (pageviews)

**Come funziona**: 
- Usiamo una scala logaritmica per gestire la differenza tra articoli di nicchia e articoli virali
- Un articolo con 1.000 views non vale "10 volte" uno con 100 views, ma riceve un incremento proporzionato
- Questo evita che gli articoli virali "schiaccino" tutti gli altri

**Esempio pratico**:
- Articolo A: 500 views → Reach Score: 45/100
- Articolo B: 5.000 views → Reach Score: 72/100
- Articolo C: 50.000 views → Reach Score: 89/100

#### 2. **Engagement** (35% del punteggio finale)
**Cosa misura**: La qualità dell'interazione con il contenuto

**Fonte dati**: Engagement Rate di Google Analytics 4
- Percentuale di sessioni in cui l'utente ha interagito attivamente (scroll, click, permanenza >10 secondi)

**Come funziona**:
- Valore da 0% a 100%
- Più alto = contenuto più coinvolgente
- Viene confrontato con tutti gli altri articoli per assegnare il punteggio relativo

**Esempio pratico**:
- Articolo A: 20% engagement → Engagement Score: 35/100
- Articolo B: 60% engagement → Engagement Score: 78/100
- Articolo C: 85% engagement → Engagement Score: 95/100

#### 3. **Depth** (30% del punteggio finale)
**Cosa misura**: Quanto tempo gli utenti dedicano all'articolo

**Fonte dati**: Average Session Duration (tempo medio di sessione)

**Come funziona**:
- Tempo in secondi che l'utente trascorre sulla pagina
- Più alto = contenuto più approfondito/interessante
- Anche qui usiamo un confronto relativo tra tutti gli articoli

**Esempio pratico**:
- Articolo A: 45 secondi → Depth Score: 30/100
- Articolo B: 180 secondi (3 min) → Depth Score: 68/100
- Articolo C: 420 secondi (7 min) → Depth Score: 92/100

### Il Calcolo Finale

```
Editorial Score = (Reach × 35%) + (Engagement × 35%) + (Depth × 30%)
```

**Esempio Completo**:

| Articolo | Reach | Engagement | Depth | **Score Finale** |
|----------|-------|------------|-------|------------------|
| "Recensione Oscar" | 72 | 78 | 68 | **73.1** |
| "News Festival" | 45 | 35 | 30 | **37.5** |
| "Approfondimento Auteur" | 35 | 95 | 92 | **74.6** |

---

## 🏆 Editorial Rank: La Classifica

Oltre allo score numerico, ogni articolo riceve un **Editorial Rank** (posizione in classifica):

- **Rank 1** = Miglior articolo del periodo
- **Rank 2** = Secondo miglior articolo
- ...e così via

Il rank è più facile da interpretare: **più basso il numero, migliore la performance**.

---

## 📊 Come Interpretare i Punteggi?

### Scala di Riferimento

| Score | Significato | Azione Consigliata |
|-------|-------------|-------------------|
| **80-100** | 🌟 **Eccellente** | Contenuto di punta. Analizzare per replicare il successo |
| **60-79** | ✅ **Buono** | Performance solida. Continuare su questa linea |
| **40-59** | ⚠️ **Nella media** | Spazio per miglioramenti. Testare variazioni |
| **20-39** | 🔧 **Sotto la media** | Necessita ottimizzazione o cambio strategia |
| **0-19** | ❌ **Critico** | Rivedere approccio editoriale per questa tipologia |

### Segmenti Automatici

Il sistema classifica automaticamente ogni articolo in **5 segmenti**:

#### 🌟 **Top Performer**
- **Score**: ≥ 80
- **Caratteristiche**: Eccelle in tutte e tre le dimensioni
- **Strategia**: Amplifica e Replica
  - Promuovi questi articoli su tutti i canali
  - Analizza cosa li rende efficaci
  - Crea contenuti simili o follow-up
  - Usali come template per linee guida editoriali

#### 🚀 **Rising Star**
- **Score**: 60-79
- **Caratteristiche**: Performance equilibrata e in crescita
- **Strategia**: Nutri la Crescita
  - Aumenta il supporto editoriale
  - Crea serie di contenuti correlati
  - Coinvolgi la community attorno a questi temi
  - Monitora per promozione a Top Performer

#### 💎 **Niche Value**
- **Score**: Variabile
- **Caratteristiche**: Engagement altissimo, anche con poco traffico
- **Strategia**: Espandi la Portata
  - Questi contenuti piacciono molto, ma poca gente li trova
  - Aumenta il budget promozionale
  - Ottimizza SEO per maggiore discoverabilità
  - Considera campagne social o newsletter dedicate

#### ⚙️ **Standard**
- **Score**: 40-59
- **Caratteristiche**: Performance ordinaria
- **Strategia**: Mantieni e Monitora
  - Continua la cadenza di pubblicazione regolare
  - Cerca opportunità di miglioramento incrementale
  - Testa strategie promozionali diverse

#### 🔧 **Underperforming**
- **Score**: < 40
- **Caratteristiche**: Basso score e basso engagement
- **Strategia**: Ottimizza o Reindirizza
  - Rivedi e migliora la qualità del contenuto
  - Ottimizza velocità di caricamento e UX
  - Aggiungi link interni rilevanti
  - Valuta content refresh o archiviazione
  - Verifica coerenza tra titolo e contenuto

---

## 💡 Come Usare lo Score nelle Decisioni Editoriali

### 1. **Planning Mensile**
Analizza i Top Performer del mese precedente:
- Quali temi hanno funzionato meglio?
- Che tipo di formato (news, recensione, approfondimento)?
- Chi sono gli autori più efficaci?

### 2. **Ottimizzazione Contenuti**
Identifica gli Underperforming:
- Il problema è la portata (poche views)? → Migliora SEO e promozione
- Il problema è l'engagement (basso coinvolgimento)? → Rivedi qualità e formato
- Il problema è la depth (poco tempo di lettura)? → Arricchisci il contenuto

### 3. **Allocazione Risorse**
- **Budget promozionale**: Investi di più sui Niche Value (alto potenziale)
- **Tempo editoriale**: Dedica più energie ai temi dei Top Performer
- **Training team**: Analizza i pattern di successo e condividili

### 4. **Benchmark Competitivo**
Confronta i tuoi contenuti con la concorrenza:
- I tuoi Top Performer sono comparabili ai loro?
- Dove c'è gap di performance?
- Quali nicchie editoriali presidi meglio?

---

## ⚠️ Cosa NON è l'Editorial Score

### ❌ Non è una metrica assoluta
Lo score è **relativo** agli altri articoli del dataset. Un articolo con score 70 non vale "70% del massimo possibile", ma è "migliore del 70% degli altri articoli".

### ❌ Non è l'unico criterio di successo
L'Editorial Score misura la **performance** ma non considera:
- Allineamento con la linea editoriale
- Importanza strategica del tema
- Contributo alla brand reputation
- Valore giornalistico intrinseco

Un'inchiesta importante può avere score medio ma enorme valore editoriale.

### ❌ Non penalizza il recente
Il sistema usa ranking basato su percentili, quindi articoli nuovi con poche views non sono automaticamente penalizzati. Vengono confrontati proporzionalmente.

---

## 🔍 Domande Frequenti

### **D: Perché il mio articolo con 10.000 views ha score più basso di uno con 1.000 views?**
**R**: Perché lo score combina tre dimensioni. L'articolo con meno views può avere engagement e depth molto più alti. È progettato per premiare la **qualità complessiva**, non solo il volume.

### **D: Come faccio a migliorare lo score dei miei articoli?**
**R**: 
1. **Per Reach**: Ottimizza SEO, usa titoli efficaci, promuovi sui social
2. **Per Engagement**: Scrivi contenuti coinvolgenti, usa multimedia, facilita la lettura
3. **Per Depth**: Crea contenuti approfonditi, articola bene la struttura, mantieni l'interesse

### **D: Lo score cambia nel tempo?**
**R**: Sì, viene ricalcolato periodicamente con i dati aggiornati. Un articolo può salire o scendere in classifica man mano che accumula performance.

### **D: Posso confrontare score di periodi diversi?**
**R**: Con cautela. Il sistema è ottimizzato per confronti **all'interno dello stesso dataset**. Per trend temporali, è meglio guardare i rank piuttosto che gli score assoluti.

### **D: Cosa significa se ho tanti articoli con anomaly_flag?**
**R**: Il flag indica incoerenze statistiche (es. score altissimo con traffico bassissimo). Non è necessariamente negativo - può indicare contenuti di nicchia di qualità. Va analizzato caso per caso.

---

## 📈 Best Practices per Massimizzare lo Score

### ✅ Per Tutti i Tipi di Contenuto

1. **Titoli Efficaci**: Chiari, informativi, SEO-friendly
2. **Struttura Leggibile**: Paragrafi brevi, sottotitoli, liste
3. **Multimedia**: Immagini, video, infografiche
4. **Call-to-Action**: Link interni, contenuti correlati
5. **Mobile-First**: Ottimizza per lettura su smartphone

### ✅ Per News e Breaking

- **Focus su Reach**: Pubblica velocemente, ottimizza per ricerca
- Titoli chiari con keyword primarie
- Aggiornamenti frequenti per mantenere rilevanza

### ✅ Per Recensioni e Approfondimenti

- **Focus su Engagement e Depth**: Contenuti ricchi e coinvolgenti
- Lunghezza adeguata (1500-2500 parole)
- Analisi dettagliate con opinioni personali
- Box riassuntivi per "quick readers"

### ✅ Per Interviste e Features

- **Bilanciamento**: Unisci portata e profondità
- Quote rilevanti evidenziate
- Aneddoti e backstory interessanti
- Condivisione sui social dell'intervistato

---

## 📊 Report e Monitoraggio

### Dati Disponibili nel Report Settimanale

Ogni settimana ricevi:
- **Top 100 Articoli** ordinati per Editorial Rank
- **Segmento** di appartenenza per ogni articolo
- **Score Dettagliato** con breakdown delle tre componenti
- **Anomaly Flags** per situazioni da approfondire

### Metriche Aggregate

- **Media Score** del periodo
- **Distribuzione per Segmento** (quanti Top Performer, Rising Star, etc.)
- **Statistiche per Categoria** (News, Recensioni, etc.)

---

## 🎯 Obiettivi Consigliati

### Per il Team
- **60%** degli articoli con score ≥ 50
- **20%** di Top Performer (score ≥ 80)
- **< 10%** di Underperforming (score < 30)

### Per Singolo Autore
- Media score personale ≥ 55
- Almeno 2 Top Performer al mese
- Crescita mese-su-mese dello score medio

---

## 📞 Supporto

Per domande sull'Editorial Score:
- **Team Analytics**: [analytics@taxidrivers.it]
- **Documentazione tecnica**: Vedi `REFACTORING_SUMMARY.md` per dettagli implementazione
- **Meeting mensile**: Review collettiva delle performance

---

**Ricorda**: L'Editorial Score è uno **strumento di supporto** alle decisioni editoriali, non un vincolo assoluto. L'obiettivo è produrre contenuti di qualità che servano i lettori, e lo score ci aiuta a capire cosa funziona meglio.

---

*Documento preparato dal Data Engineering Team - Havas Analytics*  
*Ultimo aggiornamento: Dicembre 2025*
