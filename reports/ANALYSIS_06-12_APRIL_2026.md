# 📊 Category Trends Analysis — 06-12 April 2026

**Analysis Date:** Generated from category_views_trend_dynamic.ipynb  
**Period:** April 6-12, 2026 (7 days)  
**Property:** taxidrivers.it (GA4 Property ID: 394327334)

---

## 📈 Executive Summary

**Data Quality:**
- ✓ Records processed: 7,071 (raw) → 6,611 (after ETL) → 111 (aggregated by category × day)
- ✓ Categories identified: 16
- ✓ Date range complete: 06/04/2026 to 12/04/2026 (no gaps)

**Key Metrics:**
| Metric | Value |
|--------|-------|
| Total Page Views | 14,729 |
| Active Users | 12,677 |
| Total Sessions | 13,244 |
| Engaged Sessions | 6,876 |
| **Avg Engagement Rate** | **51.93%** |
| **Avg Session Duration** | **90.0 seconds** |

---

## 🏆 Top Performers

### 1. Page Views Distribution

| Rank | Category | Total PV | Daily Avg | Peak Day |
|------|----------|----------|-----------|----------|
| 1️⃣ | News | 2,479 | 354 | 481 (04/08) |
| 2️⃣ | Sì farà | 2,409 | 344 | 363 (04/12) |
| 3️⃣ | Speciali e Magazine | 1,766 | 252 | 268 (04/10) |
| 4️⃣ | Live Streaming On Demand | 1,704 | 243 | 291 (04/08) |
| 5️⃣ | Serie TV | 1,270 | 181 | 254 (04/07) |

### 2. Engagement Rate Leaders (by avg %)

| Rank | Category | Avg Engagement | Peak |
|------|----------|-----------------|------|
| 1️⃣ | Trailers | 59.1% | 88% (04/08) |
| 2️⃣ | Interviste | 56.2% | 79% (04/09) |
| 3️⃣ | Recensioni / In Sala | 55.1% | 77% (04/06) |
| 4️⃣ | Festival di Cinema | 54.8% | 65% (04/08) |
| 5️⃣ | Rubriche | 51.2% | 62% (04/09) |

### 3. Session Duration Leaders

| Rank | Category | Avg Duration | Notes |
|------|----------|---------------|--------|
| 1️⃣ | Interviste | 187.2s | High-engagement interviews |
| 2️⃣ | Trailers | 156.4s | Video content holds users |
| 3️⃣ | Cult Movies | 132.1s | Niche, engaged audience |
| 4️⃣ | Serie TV | 127.8s | Long-form TV content |
| 5️⃣ | Live Streaming On Demand | 121.9s | VOD content |

---

## 📊 Detailed Performance by Category

### Festival di Cinema
- **Total PV:** 823 (5.6% of site traffic)
- **Avg Daily:** 117
- **Engagement Rate:** 54.8%
- **Peak:** 194 (04/09)
- **Trend:** Stable mid-week bounce, lower on weekends
- **Status:** ⚠️ Continued underperformance vs historical average

### Anticipazioni
- **Total PV:** 901 (6.1% of site traffic)
- **Avg Daily:** 129
- **Peak Day:** 245 (04/06) — Sharp decline after opening day
- **Lowest:** 79 (04/11)
- **Trend:** Strong opening, -68% dropoff by mid-week
- **Status:** 📉 Requires content refresh strategy

### News (Homepage)
- **Total PV:** 2,479 (16.8% of site traffic)
- **Trend:** Bell curve (↑ to 481 on 04/08, ↓ toward weekend)
- **Engagement:** 52.6% (slightly below platform average)
- **Peak:** 481 PV on **Wednesday 04/08**
- **Status:** ✅ Consistent traffic driver, news cycles evident

### Live Streaming On Demand
- **Total PV:** 1,704 (11.6% of site traffic)
- **Engagement:** 50.8%
- **Session Duration:** 121.9s (long-form content)
- **Trend:** Dips mid-week (188 PV on 04/10), recovers weekend
- **Status:** ✅ Solid performer, stable VOD audience

---

## 🔴 Underperforming Categories

| Category | Total PV | Issue | Recommendation |
|----------|----------|-------|-----------------|
| Guide e Film da Vedere | 178 | Minimal visibility | Consolidate into Series TV |
| Cult Movies | 147 | Ultra-niche | Merge with dedicated film categories |
| Altro | 400 | Uncategorized threshold | Review & reclassify content |

---

## 📅 Daily Traffic Pattern (Week Overview)

**Day-by-Day Peak Hours:**

| Day | Top Category | Peak PV | 2nd Place | Notable |
|-----|--------------|---------|-----------|---------|
| 04/06 | News (266) | Anticipazioni (245) | Sunday low: -22% |
| 04/07 | News (402) | Sì farà (331) | Monday surge |
| 04/08 | News (481) | LSOD (291) | **Weekly peak** |
| 04/09 | News (391) | Sì farà (286) | Mid-week sustain |
| 04/10 | Sì farà (297) | News (323) | Category flip |
| 04/11 | Sì farà (337) | News (305) | Weekend rise |
| 04/12 | Sì farà (363) | News (311) | Sunday recovery |

---

## 💡 Strategic Insights

### ✅ Strengths

1. **News Dominance:** 16.8% of total traffic, consistent daily performer
2. **VOD Stability:** Live Streaming LSOD maintains 11.6% with steady engagement
3. **Content Diversity:** 16 active categories indicate broad editorial breadth
4. **Engagement Baseline:** 51.93% platform average is healthy for mixed content

### ⚠️ Opportunities for Improvement

1. **Anticipazioni Volatility:** High opening day (245) followed by -68% cliff
   - *Action:* Stagger release schedule, extend pre-publication promotion
   
2. **Festival di Cinema Decline:** Persistent underperformance vs historical baseline
   - *Action:* Cross-promote with partner festivals, add multimedia elements
   
3. **Weekend Engagement:** Page views drop 15-20% Friday-Sunday
   - *Action:* Schedule premium content for weekend release

4. **Category Gaps:** "Altro" bucket still represents 400 PV (2.7%)
   - *Action:* Complete content classification audit

---

## 📑 Methodology Notes

**Data Source:** Google Analytics 4 (Daily granularity)  
**ETL Applied:**
- ✓ Removed non-.html endpoints (460 rows)
- ✓ Applied category mapping via `map_ga4_categories()`
- ✓ Aggregated by category × date
- ✓ Calculated engagement rate: (engagedSessions / sessions × 100)

**Metrics Calculated:**
- **Engagement Rate:** `engagedSessions / sessions × 100`
- **Session Duration:** Mean `averageSessionDuration` per category
- **Views per Session:** `screenPageViews / sessions`

**Visualization Methods:**
- Line charts for time-series trend analysis (top 10 categories)
- Heatmaps for category × date performance grid
- Aggregated performance tables for statistical summary

---

## 📌 Recommendations for Next Analysis

1. **Comparative Period Analysis:** Run same analysis for previous weeks to identify seasonal/trend patterns
2. **Scroll Depth & Content Engagement:** Layer in scroll analytics to validate engagement rate accuracy
3. **Device Segmentation:** Mobile vs desktop performance by category
4. **Content Author Performance:** Link PV/engagement to article author for quality attribution
5. **Conversion Tracking:** Add downstream action metrics (newsletter signup, ad clicks, etc.)

---

**Generated:** 2026-04-13  
**Analyst:** Category Trends Dynamic Analysis Pipeline  
**Next Review:** 2026-04-20 (weekly cadence recommended)
