# Score editoriale per singolo articolo — Implementazione GA4 standard

## Scopo del documento

Questo documento definisce il metodo di calcolo dello score editoriale per singolo articolo, implementabile interamente con GA4 e il tracciamento Enhanced Measurement standard (nessun evento custom richiesto). È progettato per essere usato come contesto operativo da un chatbot che supporta l'analisi delle performance editoriali.

---

## Principio generale

Lo score valuta ogni articolo su tre dimensioni indipendenti: quante persone lo hanno raggiunto, quanto lo hanno consumato davvero, e cosa hanno fatto dopo. Il risultato finale è un numero da 0 a 100.

Le pageviews pesano poco (15%): misurano il clic, non la lettura.

---

## Formula

```
Score = (Portata × 0,15) + (Qualità di lettura × 0,40)
       + (Completamento × 0,25) + (Recirculation × 0,20)
```

Ogni componente è normalizzata su scala 0–100 prima di essere inserita nella formula.

---

## Componenti dello score

### 1. Portata — peso 15%

**Cosa misura:** la diffusione relativa dell'articolo rispetto alla media del sito nel periodo.

**Metrica GA4:** `Views` (pageviews per pagina).

**Dove trovarla:** Reports > Engagement > Pages and Screens. Filtrare per page path e periodo.

**Calcolo:**
```
Portata = (views articolo / media views articoli del sito nello stesso periodo) × 100
```
Cappare a 100 se il rapporto supera 1.

**Esempio:** articolo con 950 views, media sito 400 views → Portata = min(237, 100) = 100.

---

### 2. Qualità di lettura — peso 40%

**Cosa misura:** quanto tempo i lettori hanno trascorso attivamente sull'articolo rispetto al tempo che ci vorrebbe per leggerlo integralmente.

**Metrica GA4:** `Average engagement time` per pagina.

GA4 misura il tempo in cui la scheda è in primo piano e l'utente è attivo (non il tempo totale di sessione). È il proxy più vicino all'attention time disponibile senza tracciamento custom.

**Dove trovarla:** Reports > Engagement > Pages and Screens, colonna "Average engagement time".

**Tempo atteso di lettura:** calcolato dalla redazione a partire dal conteggio parole dell'articolo, con velocità di lettura standard di 200 parole al minuto.

```
Tempo atteso (secondi) = (numero parole ÷ 200) × 60
```

**Calcolo:**
```
Qualità di lettura = (average engagement time articolo / tempo atteso lettura) × 100
```
Cappare a 100.

**Esempio:** articolo da 900 parole → tempo atteso 270 secondi (4,5 min). Average engagement time = 150 secondi → Qualità = (150/270) × 100 = 55,6.

**Nota operativa:** il conteggio parole va fornito dalla redazione o estratto dal CMS. Non è disponibile in GA4.

---

### 3. Completamento — peso 25%

**Cosa misura:** la percentuale di lettori che ha raggiunto la fine dell'articolo.

**Metrica GA4:** evento `scroll` con parametro `percent_scrolled = 90`. GA4 Enhanced Measurement attiva automaticamente questo evento quando un utente raggiunge il 90% della pagina. Non richiede codice custom.

**Dove trovarla:** Reports > Engagement > Events, filtrare per event name "scroll". Per isolare una pagina specifica: Explore > Free Form, dimensione "Page path", metrica "Event count" filtrata su event name = scroll. Dividere per le views della stessa pagina nello stesso periodo.

**Calcolo:**
```
Tasso di completamento = (scroll events 90% su articolo / views articolo) × 100
```

**Scala di punteggio:**

| Tasso di completamento | Punti |
|---|---|
| 70% e oltre | 100 |
| 50–69% | 75 |
| 30–49% | 45 |
| 15–29% | 20 |
| sotto 15% | 5 |

**Nota tecnica:** GA4 Enhanced Measurement traccia un solo threshold (90%). Non è un gradiente continuo, ma è sufficiente per distinguere articoli letti da articoli abbandonati. Per un gradiente 25/50/75/90% servirebbe tracciamento custom.

---

### 4. Recirculation — peso 20%

**Cosa misura:** la capacità dell'articolo di trattenere il lettore sul sito, generando almeno una lettura aggiuntiva.

**Metrica GA4 (metodo diretto — richiede Explore):**
Usare Explore > Free Form con:
- Dimensione: Landing page (filtrare per URL articolo)
- Metrica: Sessions, Engaged sessions

Il tasso di recirculation è approssimabile dall'engagement rate delle sessioni che atterrano sull'articolo: una sessione "engaged" in GA4 dura più di 10 secondi, include almeno 2 pageviews o un evento di conversione. Le sessioni con 2+ pageviews indicano recirculation.

```
Recirculation = (sessioni con 2+ pageviews avviate sull'articolo / 
                 sessioni totali avviate sull'articolo) × 100
```

**Metodo semplificato (solo report standard):**
Se non si dispone di Explore, usare l'engagement rate delle sessioni con landing page sull'articolo come proxy. Estrarre da Reports > Acquisition > Traffic acquisition, segmentando per landing page.

**Scala di punteggio:**

| Tasso di recirculation | Punti |
|---|---|
| 45% e oltre | 100 |
| 30–44% | 75 |
| 20–29% | 50 |
| 10–19% | 25 |
| sotto 10% | 5 |

**Benchmark di riferimento:** per un magazine di cinema e TV, un tasso sano si colloca tra il 30% e il 45%. Sotto il 20% segnala disallineamento tra il pubblico attratto dall'articolo e il posizionamento editoriale del sito.

---

## Esempio di calcolo completo

| Componente | Valore grezzo | Punteggio (0–100) | Peso | Contributo |
|---|---|---|---|---|
| Portata | 950 views (media 400) | 100 | 15% | 15,0 |
| Qualità di lettura | 150s / 270s attesi | 55,6 | 40% | 22,2 |
| Completamento | 42% scroll 90% | 45 | 25% | 11,3 |
| Recirculation | 28% | 50 | 20% | 10,0 |
| **Score finale** | | | | **58,5** |

---

## Fasce di valutazione

| Score | Fascia | Interpretazione |
|---|---|---|
| 80–100 | Eccellente | Articolo ad alta portata e alta qualità di consumo |
| 60–79 | Buono | Buon equilibrio tra reach e lettura effettiva |
| 40–59 | Nella media | Portata o qualità di lettura da migliorare |
| 20–39 | Sotto la media | Problema strutturale su una o più dimensioni |
| 0–19 | Critico | Articolo non letto o che disperde audience |

---

## Limitazioni note dell'implementazione GA4 standard

| Limitazione | Impatto | Soluzione futura |
|---|---|---|
| Scroll depth binario (solo 90%) | Il completamento non è un gradiente | Aggiungere eventi custom a 25/50/75% |
| Average engagement time è per sessione, non solo per pagina | Leggera sovrastima su sessioni multi-pagina | Accettabile come proxy per articoli letti come landing page |
| Recirculation richiede Explore per essere precisa | Il metodo semplificato è meno granulare | Usare Explore come metodo primario |
| Il conteggio parole non è in GA4 | La qualità di lettura richiede input manuale o da CMS | Integrare il dato dal CMS via custom dimension |

---

## Input richiesti per il calcolo

Per ogni articolo da valutare, il chatbot deve ricevere:

1. **Views** dell'articolo nel periodo (da GA4 Pages and Screens)
2. **Media views** degli articoli del sito nello stesso periodo
3. **Average engagement time** dell'articolo (da GA4 Pages and Screens)
4. **Numero di parole** dell'articolo (dalla redazione o dal CMS)
5. **Tasso scroll 90%** — scroll events / views (da GA4 Explore o Events)
6. **Tasso di recirculation** — sessioni 2+ pageviews / sessioni totali su landing page (da GA4 Explore)

---

## Istruzioni operative per il chatbot

Quando ricevi i dati di un articolo:

1. Calcola il punteggio di ogni componente secondo le formule sopra.
2. Applica i pesi e somma i contributi.
3. Indica la fascia di appartenenza dello score finale.
4. Segnala le componenti con punteggio più basso come aree prioritarie di intervento.
5. Se manca il conteggio parole, chiedi esplicitamente: senza questo dato la qualità di lettura non è calcolabile.
6. Se manca il tasso di recirculation, usa come proxy l'engagement rate GA4 delle sessioni con quella landing page, indicando che si tratta di un'approssimazione.
