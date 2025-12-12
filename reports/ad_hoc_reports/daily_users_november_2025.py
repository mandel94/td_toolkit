"""
Daily Active Users - November 2025 Report
Fetches daily active users data from GA4 for November 2025 and creates:
1. CSV report with daily data
2. Seaborn visualization
3. Average daily users calculation
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from ga4_api.ga4_api import Ga4Client
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime

# GA4 Property ID
PROPERTY_ID = '394327334'

# Output paths
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

CSV_OUTPUT = os.path.join(OUTPUT_DIR, 'daily_users_november_2025.csv')
CHART_OUTPUT = os.path.join(OUTPUT_DIR, 'daily_users_november_2025_chart.png')

def fetch_daily_users_november_2025():
    """Fetch daily active users for November 2025"""
    print("Initializing GA4 Client...")
    ga4 = Ga4Client()
    
    # November 2025 date range
    start_date = "2025-11-01"
    end_date = "2025-11-30"
    
    print(f"Fetching daily data for {start_date} to {end_date}...")
    
    df = ga4.run_query(
        property_id=PROPERTY_ID,
        dimensions=['date'],  # Daily dimension
        metrics=['activeUsers', 'totalUsers'],
        start_date=start_date,
        end_date=end_date
    )
    
    if not df.empty:
        # Convert date from YYYYMMDD to readable format
        df['date_formatted'] = pd.to_datetime(df['date'], format='%Y%m%d')
        df['day'] = df['date_formatted'].dt.day
        df['day_name'] = df['date_formatted'].dt.day_name()
        df['active_users'] = pd.to_numeric(df['activeUsers'])
        df['total_users'] = pd.to_numeric(df['totalUsers'])
        
        # Sort by date
        df = df.sort_values('date_formatted')
        
        # Select and reorder columns
        df_output = df[['date_formatted', 'day', 'day_name', 'active_users', 'total_users']].copy()
        df_output.columns = ['date', 'day', 'day_name', 'active_users', 'total_users']
        
        print(f"✓ Retrieved data for {len(df_output)} days")
        return df_output
    else:
        print("✗ No data available")
        return pd.DataFrame()

def save_csv(df):
    """Save results to CSV"""
    df.to_csv(CSV_OUTPUT, index=False, encoding='utf-8')
    print(f"\n✓ CSV saved to: {CSV_OUTPUT}")

def create_visualization(df):
    """Create a beautiful Seaborn visualization"""
    # Set the style
    sns.set_style("whitegrid")
    sns.set_palette("deep")
    
    # Create figure with larger size
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10))
    
    # Plot 1: Line plot with trend
    sns.lineplot(
        data=df,
        x='day',
        y='active_users',
        ax=ax1,
        marker='o',
        linewidth=2.5,
        markersize=8,
        color='#3498db'
    )
    
    # Add average line
    avg_users = df['active_users'].mean()
    ax1.axhline(y=avg_users, color='red', linestyle='--', linewidth=2, label=f'Average: {avg_users:,.0f}')
    
    ax1.set_title('Daily Active Users - November 2025', fontsize=18, fontweight='bold', pad=20)
    ax1.set_xlabel('Day of Month', fontsize=14, fontweight='bold')
    ax1.set_ylabel('Active Users', fontsize=14, fontweight='bold')
    ax1.legend(fontsize=12)
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
    ax1.grid(axis='y', alpha=0.3, linestyle='--')
    
    # Plot 2: Bar plot by day of week
    day_of_week_avg = df.groupby('day_name')['active_users'].mean().reindex([
        'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'
    ])
    
    bars = sns.barplot(
        x=day_of_week_avg.index,
        y=day_of_week_avg.values,
        ax=ax2,
        palette='viridis',
        alpha=0.8
    )
    
    # Add value labels on bars
    for i, v in enumerate(day_of_week_avg.values):
        if not pd.isna(v):
            bars.text(i, v, f'{v:,.0f}', ha='center', va='bottom', fontsize=11, fontweight='bold')
    
    ax2.set_title('Average Active Users by Day of Week - November 2025', fontsize=16, fontweight='bold', pad=20)
    ax2.set_xlabel('Day of Week', fontsize=14, fontweight='bold')
    ax2.set_ylabel('Average Active Users', fontsize=14, fontweight='bold')
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
    ax2.grid(axis='y', alpha=0.3, linestyle='--')
    ax2.set_axisbelow(True)
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # Adjust layout
    plt.tight_layout()
    
    # Save the figure
    plt.savefig(CHART_OUTPUT, dpi=300, bbox_inches='tight')
    print(f"✓ Chart saved to: {CHART_OUTPUT}")
    
    # Display the plot
    plt.show()

def main():
    """Main execution function"""
    print("=" * 70)
    print("DAILY ACTIVE USERS - NOVEMBER 2025 - GA4 REPORT")
    print("=" * 70)
    print()
    
    # Fetch data
    df = fetch_daily_users_november_2025()
    
    if df.empty:
        print("\n✗ No data retrieved. Please check your GA4 connection.")
        return
    
    # Save to CSV
    save_csv(df)
    
    # Create visualization
    create_visualization(df)
    
    # Print summary statistics
    print("\n" + "=" * 70)
    print("SUMMARY STATISTICS - NOVEMBER 2025")
    print("=" * 70)
    print(f"Total days analyzed: {len(df)}")
    print(f"\nAVERAGE DAILY ACTIVE USERS: {df['active_users'].mean():,.2f}")
    print(f"Median daily active users: {df['active_users'].median():,.0f}")
    print(f"Standard deviation: {df['active_users'].std():,.2f}")
    print(f"\nPeak day: {df.loc[df['active_users'].idxmax(), 'date'].strftime('%Y-%m-%d')} ({df.loc[df['active_users'].idxmax(), 'day_name']}) - {df['active_users'].max():,} users")
    print(f"Lowest day: {df.loc[df['active_users'].idxmin(), 'date'].strftime('%Y-%m-%d')} ({df.loc[df['active_users'].idxmin(), 'day_name']}) - {df['active_users'].min():,} users")
    
    # Day of week analysis
    print("\n" + "-" * 70)
    print("AVERAGE BY DAY OF WEEK:")
    print("-" * 70)
    day_of_week_avg = df.groupby('day_name')['active_users'].mean().reindex([
        'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'
    ])
    for day, avg in day_of_week_avg.items():
        if not pd.isna(avg):
            print(f"{day:12s}: {avg:>10,.2f} users")
    
    print("=" * 70)

if __name__ == "__main__":
    main()
