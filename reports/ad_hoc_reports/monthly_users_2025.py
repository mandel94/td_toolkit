"""
Monthly Users 2025 Report
Fetches monthly active users data from GA4 for 2025 and creates:
1. CSV report
2. Seaborn visualization
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

CSV_OUTPUT = os.path.join(OUTPUT_DIR, 'monthly_users_2025.csv')
CHART_OUTPUT = os.path.join(OUTPUT_DIR, 'monthly_users_2025_chart.png')

def get_month_date_range(year, month):
    """Get the first and last day of a month"""
    from calendar import monthrange
    last_day = monthrange(year, month)[1]
    start_date = f"{year}-{month:02d}-01"
    end_date = f"{year}-{month:02d}-{last_day}"
    return start_date, end_date

def fetch_monthly_users_2025():
    """Fetch monthly active users for each month in 2025"""
    print("Initializing GA4 Client...")
    ga4 = Ga4Client()
    
    results = []
    current_month = datetime.now().month if datetime.now().year == 2025 else 12
    
    # Get data for each month in 2025 (up to current month)
    for month in range(1, current_month + 1):
        start_date, end_date = get_month_date_range(2025, month)
        
        print(f"Fetching data for {start_date} to {end_date}...")
        
        df = ga4.run_query(
            property_id=PROPERTY_ID,
            dimensions=None,  # No dimensions, just aggregate metrics
            metrics=['activeUsers', 'totalUsers'],
            start_date=start_date,
            end_date=end_date
        )
        
        if not df.empty:
            month_name = datetime(2025, month, 1).strftime('%B')
            results.append({
                'month': month,
                'month_name': month_name,
                'month_year': f"{month_name} 2025",
                'active_users': int(df['activeUsers'].iloc[0]),
                'total_users': int(df['totalUsers'].iloc[0]),
                'start_date': start_date,
                'end_date': end_date
            })
            print(f"  ✓ {month_name}: {int(df['activeUsers'].iloc[0]):,} active users")
        else:
            print(f"  ✗ No data available for month {month}")
    
    return pd.DataFrame(results)

def save_csv(df):
    """Save results to CSV"""
    df.to_csv(CSV_OUTPUT, index=False, encoding='utf-8')
    print(f"\n✓ CSV saved to: {CSV_OUTPUT}")

def create_visualization(df):
    """Create a beautiful Seaborn visualization"""
    # Set the style
    sns.set_style("whitegrid")
    sns.set_palette("husl")
    
    # Create figure with larger size for better readability
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Create bar plot
    bars = sns.barplot(
        data=df,
        x='month_name',
        y='active_users',
        ax=ax,
        alpha=0.8
    )
    
    # Add value labels on top of bars
    for i, (idx, row) in enumerate(df.iterrows()):
        bars.text(
            i, 
            row['active_users'], 
            f"{row['active_users']:,}", 
            ha='center', 
            va='bottom',
            fontsize=11,
            fontweight='bold'
        )
    
    # Customize the plot
    ax.set_title('Monthly Active Users - 2025', fontsize=18, fontweight='bold', pad=20)
    ax.set_xlabel('Month', fontsize=14, fontweight='bold')
    ax.set_ylabel('Active Users', fontsize=14, fontweight='bold')
    
    # Format y-axis with thousand separators
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
    
    # Rotate x-axis labels for better readability
    plt.xticks(rotation=45, ha='right')
    
    # Add a subtle grid
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    ax.set_axisbelow(True)
    
    # Adjust layout to prevent label cutoff
    plt.tight_layout()
    
    # Save the figure
    plt.savefig(CHART_OUTPUT, dpi=300, bbox_inches='tight')
    print(f"✓ Chart saved to: {CHART_OUTPUT}")
    
    # Display the plot
    plt.show()

def main():
    """Main execution function"""
    print("=" * 60)
    print("MONTHLY ACTIVE USERS 2025 - GA4 REPORT")
    print("=" * 60)
    print()
    
    # Fetch data
    df = fetch_monthly_users_2025()
    
    if df.empty:
        print("\n✗ No data retrieved. Please check your GA4 connection.")
        return
    
    # Save to CSV
    save_csv(df)
    
    # Create visualization
    create_visualization(df)
    
    # Print summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total months analyzed: {len(df)}")
    print(f"Average monthly users: {df['active_users'].mean():,.0f}")
    print(f"Total users (sum): {df['active_users'].sum():,}")
    print(f"Peak month: {df.loc[df['active_users'].idxmax(), 'month_name']} ({df['active_users'].max():,} users)")
    print(f"Lowest month: {df.loc[df['active_users'].idxmin(), 'month_name']} ({df['active_users'].min():,} users)")
    print("=" * 60)

if __name__ == "__main__":
    main()
