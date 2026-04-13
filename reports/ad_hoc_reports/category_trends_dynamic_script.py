#!/usr/bin/env python
"""
Analisi Category Trends con Date Range Dinamico (06-12 Aprile 2026)
Carica dati da GA4 per il periodo specificato e fa analisi per categoria.
"""

import os
import sys
from datetime import datetime, date, timedelta
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from scipy import stats

# Path setup
_PROJECT_ROOT = os.path.abspath(os.path.join(os.getcwd(), '..', '..'))
for _p in (_PROJECT_ROOT, os.path.join(_PROJECT_ROOT, 'reports')):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# =====================================================================
# CONFIGURAZIONE PARAMETRI
# =====================================================================

START_DATE = date(2026, 4, 6)
END_DATE = date(2026, 4, 12)
PROPERTY_ID = '394327334'
DOMAIN = 'https://taxidrivers.it'
GRANULARITY = 'Daily'  # Daily, Weekly, Monthly

print(f"{'='*70}")
print(f"📊 ANALISI CATEGORY TRENDS — Date Range Dinamico")
print(f"{'='*70}")
print(f"📅 Periodo: {START_DATE.strftime('%d/%m/%Y')} → {END_DATE.strftime('%d/%m/%Y')}")
print(f"📈 Granularità: {GRANULARITY}")
print(f"{'='*70}\n")

# =====================================================================
# LOAD DATA DA GA4
# =====================================================================

try:
    from ga4_api.ga4_api import Ga4Client
    from etl.page_and_screen_etl import PageAndScreenETLFactory
    from map_ga4_categories import map_ga4_categories
    
    print("🔄 Step 1: Interrogazione GA4 API...")
    ga4 = Ga4Client()
    df = ga4.run_query(
        property_id=PROPERTY_ID,
        dimensions=['pagePath', 'date'],
        metrics=['screenPageViews', 'activeUsers', 'engagedSessions', 'sessions', 'averageSessionDuration'],
        start_date=START_DATE.strftime('%Y-%m-%d'),
        end_date=END_DATE.strftime('%Y-%m-%d')
    )
    print(f"✓ Dati caricati: {len(df)} righe\n")
    
    # ETL Pulizia
    print("🔄 Step 2: ETL Pulizia dati...")
    etl = PageAndScreenETLFactory.get_etl('en', df=df)
    etl.apply_transformations()
    df = etl.df
    print(f"✓ Dopo pulizia: {len(df)} righe\n")
    
    # Parse date
    df['date_parsed'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date_parsed'])
    
    # Map categoria
    print("🔄 Step 3: Mapping categorie...")
    df['category'] = df['pagePath'].apply(map_ga4_categories)
    print(f"✓ Categorie trovate: {df['category'].nunique()}\n")
    
    # Converti metriche
    for col in ['screenPageViews', 'activeUsers', 'engagedSessions', 'sessions', 'averageSessionDuration']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    print(f"✓ Dati pronti!\n")
    
except Exception as e:
    print(f"❌ Errore durante il caricamento: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# =====================================================================
# AGGREGAZIONE PER CATEGORIA
# =====================================================================

print("🔄 Step 4: Aggregazione dati...")
if GRANULARITY == 'Daily':
    agg_df = df.groupby(['category', 'date_parsed']).agg({
        'screenPageViews': 'sum',
        'activeUsers': 'sum',
        'engagedSessions': 'sum',
        'sessions': 'sum',
        'averageSessionDuration': 'mean'
    }).reset_index()
    time_label = 'Data'
elif GRANULARITY == 'Weekly':
    df['week'] = df['date_parsed'].dt.to_period('W').apply(lambda x: x.start_time)
    agg_df = df.groupby(['category', 'week']).agg({
        'screenPageViews': 'sum',
        'activeUsers': 'sum',
        'engagedSessions': 'sum',
        'sessions': 'sum',
        'averageSessionDuration': 'mean'
    }).reset_index()
    agg_df.rename(columns={'week': 'date_parsed'}, inplace=True)
    time_label = 'Settimana'
else:  # Monthly
    df['month'] = df['date_parsed'].dt.to_period('M').apply(lambda x: x.start_time)
    agg_df = df.groupby(['category', 'month']).agg({
        'screenPageViews': 'sum',
        'activeUsers': 'sum',
        'engagedSessions': 'sum',
        'sessions': 'sum',
        'averageSessionDuration': 'mean'
    }).reset_index()
    agg_df.rename(columns={'month': 'date_parsed'}, inplace=True)
    time_label = 'Mese'

# Calcola engagement rate
agg_df['engagementRate'] = (agg_df['engagedSessions'] / agg_df['sessions'] * 100).round(2)

# Pivot per visualizzazione
pivot = agg_df.pivot_table(
    index='date_parsed',
    columns='category',
    values='screenPageViews',
    aggfunc='sum'
)

print(f"✓ Aggregazione completata\n")

# =====================================================================
# STATISTICHE DI BASE
# =====================================================================

print("📊 STATISTICHE DI BASE")
print(f"{'='*70}")
print(f"Categorie presenti: {agg_df['category'].nunique()}")
print(f"Periodi di analisi: {agg_df['date_parsed'].nunique()}")
print(f"\n📈 Page Views per Categoria (TOTALI):")
print(agg_df.groupby('category')['screenPageViews'].sum().sort_values(ascending=False).head(10))

print(f"\n👥 Active Users per Categoria:")
print(agg_df.groupby('category')['activeUsers'].sum().sort_values(ascending=False).head(10))

print(f"\n💬 Engagement Rate Medio per Categoria:")
print(agg_df.groupby('category')['engagementRate'].mean().sort_values(ascending=False).head(10))

print(f"\n⏱  Average Session Duration per Categoria:")
print(agg_df.groupby('category')['averageSessionDuration'].mean().sort_values(ascending=False).head(10))

# =====================================================================
# VISUALIZZAZIONE 1: Views per Categoria over Time
# =====================================================================

print(f"\n{'='*70}")
print("📈 VISUALIZZAZIONE 1: Page Views per Categoria over Time")
print(f"{'='*70}\n")

# Stile
sns.set_theme(style="whitegrid", palette="tab20")
plt.rcParams.update({"figure.dpi": 130, "font.size": 10})

fig, ax = plt.subplots(figsize=(14, 7))

# Top 10 categorie
top_categories = agg_df.groupby('category')['screenPageViews'].sum().nlargest(10).index.tolist()

for category in top_categories:
    category_data = agg_df[agg_df['category'] == category].sort_values('date_parsed')
    ax.plot(category_data['date_parsed'], category_data['screenPageViews'], 
            marker='o', label=category, linewidth=2.5, markersize=6)

ax.set_xlabel(f'{time_label}', fontweight='bold', fontsize=11)
ax.set_ylabel('Page Views', fontweight='bold', fontsize=11)
ax.set_title(f'Page Views per Categoria ({GRANULARITY}) — {START_DATE.strftime("%d/%m/%Y")} a {END_DATE.strftime("%d/%m/%Y")}',
             fontweight='bold', fontsize=13, pad=15)
ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=9)
ax.grid(alpha=0.3)
plt.tight_layout()
plt.show()

print("✓ Visualizzazione completata\n")

# =====================================================================
# VISUALIZZAZIONE 2: Engagement Rate per Categoria
# =====================================================================

print(f"{'='*70}")
print("💬 VISUALIZZAZIONE 2: Engagement Rate per Categoria")
print(f"{'='*70}\n")

fig, ax = plt.subplots(figsize=(14, 7))

for category in top_categories:
    category_data = agg_df[agg_df['category'] == category].sort_values('date_parsed')
    ax.plot(category_data['date_parsed'], category_data['engagementRate'], 
            marker='s', label=category, linewidth=2.5, markersize=6)

ax.set_xlabel(f'{time_label}', fontweight='bold', fontsize=11)
ax.set_ylabel('Engagement Rate (%)', fontweight='bold', fontsize=11)
ax.set_title(f'Engagement Rate per Categoria ({GRANULARITY}) — {START_DATE.strftime("%d/%m/%Y")} a {END_DATE.strftime("%d/%m/%Y")}',
             fontweight='bold', fontsize=13, pad=15)
ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=9)
ax.grid(alpha=0.3)
plt.tight_layout()
plt.show()

print("✓ Visualizzazione completata\n")

# =====================================================================
# VISUALIZZAZIONE 3: Top Articles per Categoria
# =====================================================================

print(f"{'='*70}")
print("⭐ TOP ARTICLES PER CATEGORIA")
print(f"{'='*70}\n")

for category in top_categories[:5]:  # Top 5 categorie
    print(f"\n🎯 {category.upper()}")
    print("-" * 70)
    top_articles = df[df['category'] == category].groupby('pagePath').agg({
        'screenPageViews': 'sum',
        'activeUsers': 'sum',
        'averageSessionDuration': 'mean'
    }).sort_values('screenPageViews', ascending=False).head(5)
    
    for idx, (path, row) in enumerate(top_articles.iterrows(), 1):
        print(f"{idx}. {path[:80]}")
        print(f"   • Views: {int(row['screenPageViews']):,} | Users: {int(row['activeUsers']):,} | Avg Duration: {row['averageSessionDuration']:.1f}s")
    print()

# =====================================================================
# CONCLUSIONI
# =====================================================================

print(f"\n{'='*70}")
print("✅ ANALISI COMPLETATA")
print(f"{'='*70}")
print(f"Periodo analizzato: {START_DATE.strftime('%d/%m/%Y')} → {END_DATE.strftime('%d/%m/%Y')} ({(END_DATE - START_DATE).days + 1} giorni)")
print(f"Categorie analizzate: {agg_df['category'].nunique()}")
print(f"Total Page Views: {agg_df['screenPageViews'].sum():,}")
print(f"Total Active Users: {agg_df['activeUsers'].sum():,}")
print(f"Engagement Rate Medio: {agg_df['engagementRate'].mean():.2f}%")
print(f"\n💡 I dati sono pronti per ulteriori analisi!")
