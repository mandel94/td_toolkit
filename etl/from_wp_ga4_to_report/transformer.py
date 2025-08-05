import pandas as pd
from urllib.parse import urlparse
from datetime import datetime, timedelta # Added
import numpy as np # Added

class Transformer:
    def __init__(self, config):
        """
        Initializes the Transformer with a configuration dictionary.
        Args:
            config (dict): A dictionary containing 'COLUMNS_TO_KEEP'.
                           May also contain 'BUCKET_LABELS', 'N_BUCKETS',
                           'METRICS_FOR_BENCHMARK', 'METRICS_FOR_BUCKETS'.
        """
        self.columns_to_keep = config.get('COLUMNS_TO_KEEP', [])
        self.bucket_labels = config.get('BUCKET_LABELS', ["Molto Basso", "Basso", "Medio", "Alto", "Molto Alto"])
        self.n_buckets = config.get('N_BUCKETS', 5)
        
        # Defines which metrics to calculate daily benchmarks for.
        # Keys are original metric column names, values are base names for derived columns.
        self.metrics_for_benchmark = config.get("METRICS_FOR_BENCHMARK", {
            "views": "views",
            "active users": "active_users",
            "average engagement time per active user": "average_engagement_time_per_active_user"
        })
        
        # Defines which 'difference' columns should be bucketed and what the bucket columns should be named.
        # Derived from metrics_for_benchmark if not explicitly provided.
        self.metrics_to_bucket_map = config.get("METRICS_TO_BUCKET_MAP")
        if self.metrics_to_bucket_map is None:
            self.metrics_to_bucket_map = {
                f"diff_with_daily_benchmark_{base_name}": f"{base_name}_bucket"
                for original_name, base_name in self.metrics_for_benchmark.items()
            }

    def _normalize_url_path(self, url):
        """Extracts the path from a URL and removes trailing slashes."""
        if pd.isna(url):
            return None
        try:
            path = urlparse(str(url)).path
            return path.rstrip('/') if path else path
        except Exception:
            return None # Return None for invalid URLs

    def _clean_ga4_data(self, ga4_df):
        """Cleans the GA4 DataFrame."""
        if ga4_df.empty:
            return pd.DataFrame()
        
        # Rename columns for clarity and consistency
        ga4_df = ga4_df.rename(columns={
            'Percorso pagina e classe schermata': 'pagepath',
            'Visualizzazioni': 'views',
            'Utenti attivi': 'active users',
            'Visualizzazioni per utente attivo': 'views per active user',
            'Durata media del coinvolgimento per utente attivo': 'average engagement time per active user',
            'Conteggio eventi': 'event count',
        })
        
        # Ensure 'pagepath' exists before trying to normalize it
        if 'pagepath' in ga4_df.columns:
            ga4_df['pagepath'] = ga4_df['pagepath'].apply(self._normalize_url_path)
        else:
            print("Warning: 'pagepath' column not found in GA4 data. Skipping normalization.")

        # Convert relevant metrics to numeric
        # 'views' is already in metrics_for_benchmark, others are common GA4 metrics
        numeric_metrics_ga4 = list(self.metrics_for_benchmark.keys()) + ['views per active user', 'event count']
        for metric in numeric_metrics_ga4:
            if metric in ga4_df.columns:
                ga4_df[metric] = pd.to_numeric(ga4_df[metric], errors='coerce')
            elif metric in self.metrics_for_benchmark: # Only print warning if it was expected for benchmark
                print(f"Warning: Metric column '{metric}' (for benchmark) not found in GA4 data.")
        
        return ga4_df

    def _clean_wp_data(self, wp_df):
        """Cleans the WordPress DataFrame."""
        if wp_df.empty:
            return pd.DataFrame()
        
        # Normalize 'link' to 'pagepath' for merging
        if 'link' in wp_df.columns:
            wp_df['pagepath'] = wp_df['link'].apply(self._normalize_url_path)
        else:
            print("Warning: 'link' column not found in WP data. Cannot create 'pagepath' for merging.")

        # Convert 'pubdate' to datetime objects
        if 'pubdate' in wp_df.columns:
            wp_df['pubdate'] = pd.to_datetime(wp_df['pubdate'], errors='coerce')
        else:
            print("Warning: 'pubdate' column not found in WP data. Benchmark calculations will be affected.")

        # Convert Yoast Linkdex to numeric if it exists
        if '_yoast_wpseo_linkdex' in wp_df.columns:
            wp_df['_yoast_wpseo_linkdex'] = pd.to_numeric(wp_df['_yoast_wpseo_linkdex'], errors='coerce')
        
        return wp_df

    def _calculate_daily_median_benchmark(self, dataframe, timestamp_column, metric_column):
        """
        Calculates the daily median benchmark for a specified metric.
        """
        df_copy = dataframe.copy()
        if timestamp_column not in df_copy.columns:
            print(f"Warning: Timestamp column '{timestamp_column}' not found for benchmark calculation of '{metric_column}'.")
            return pd.DataFrame(columns=['date', f'daily_median_benchmark_{metric_column}'])
        if metric_column not in df_copy.columns:
            print(f"Warning: Metric column '{metric_column}' not found for benchmark calculation.")
            return pd.DataFrame(columns=['date', f'daily_median_benchmark_{metric_column}'])

        df_copy[timestamp_column] = pd.to_datetime(df_copy[timestamp_column], errors='coerce')
        df_copy.dropna(subset=[timestamp_column, metric_column], how='any', inplace=True) # Drop if date or metric is NA

        if df_copy.empty:
            return pd.DataFrame(columns=['date', f'daily_median_benchmark_{metric_column}'])

        df_copy['date'] = df_copy[timestamp_column].dt.date
        df_copy[metric_column] = pd.to_numeric(df_copy[metric_column], errors='coerce')
        
        daily_median = (
            df_copy.groupby('date', as_index=False)[metric_column]
            .median()
            .rename(columns={metric_column: f'daily_median_benchmark_{metric_column}'})
        )
        return daily_median

    def _add_benchmark_differences(self, df, pubdate_col):
        """
        Computes the difference from the daily median benchmark for configured metrics.
        """
        df_copy = df.copy()
        if pubdate_col not in df_copy.columns or df_copy[pubdate_col].isnull().all():
            print(f"Warning: Publication date column '{pubdate_col}' not found or all null. Skipping benchmark difference calculation.")
            for original_metric_name, base_name in self.metrics_for_benchmark.items():
                diff_col_name = f"diff_with_daily_benchmark_{base_name}"
                df_copy[diff_col_name] = np.nan
            return df_copy

        df_copy[pubdate_col] = pd.to_datetime(df_copy[pubdate_col], errors='coerce')
        df_copy['merge_date_key'] = df_copy[pubdate_col].dt.date

        for original_metric_name, base_name in self.metrics_for_benchmark.items():
            diff_col_name = f"diff_with_daily_benchmark_{base_name}"
            benchmark_col_for_merge = f"daily_median_benchmark_{original_metric_name}"

            if original_metric_name not in df_copy.columns:
                print(f"Warning: Metric column '{original_metric_name}' not found for benchmark difference. Adding NaN column '{diff_col_name}'.")
                df_copy[diff_col_name] = np.nan
                continue

            daily_medians_df = self._calculate_daily_median_benchmark(df_copy, pubdate_col, original_metric_name)

            if daily_medians_df.empty or 'date' not in daily_medians_df.columns or benchmark_col_for_merge not in daily_medians_df.columns:
                print(f"Warning: No daily medians or benchmark column missing for '{original_metric_name}'. Diff column '{diff_col_name}' will be NaN.")
                df_copy[diff_col_name] = np.nan
                continue
            
            daily_medians_df['date'] = pd.to_datetime(daily_medians_df['date']).dt.date
            
            df_copy = pd.merge(df_copy, daily_medians_df, left_on='merge_date_key', right_on='date', how='left', suffixes=('', '_bm_median'))
            
            if benchmark_col_for_merge in df_copy.columns:
                df_copy[original_metric_name] = pd.to_numeric(df_copy[original_metric_name], errors='coerce')
                df_copy[benchmark_col_for_merge] = pd.to_numeric(df_copy[benchmark_col_for_merge], errors='coerce')
                df_copy[diff_col_name] = df_copy[original_metric_name] - df_copy[benchmark_col_for_merge]
                df_copy.drop(columns=[benchmark_col_for_merge], inplace=True, errors='ignore')
            else:
                print(f"Warning: Benchmark column '{benchmark_col_for_merge}' not found after merge for '{original_metric_name}'. Diff column '{diff_col_name}' will be NaN.")
                df_copy[diff_col_name] = np.nan
            
            if 'date_bm_median' in df_copy.columns: df_copy.drop(columns=['date_bm_median'], inplace=True, errors='ignore')
            if 'date' in df_copy.columns and 'merge_date_key' in df_copy.columns and df_copy['date'].equals(df_copy['merge_date_key']): # if 'date' was from merge
                 pass # keep the original 'date' if it was just the merge key
            elif 'date' in df_copy.columns and 'merge_date_key' in df_copy.columns and not df_copy['date'].equals(df_copy['merge_date_key']): # if 'date' was from merge and different
                 df_copy.drop(columns=['date'], inplace=True, errors='ignore')


        if 'merge_date_key' in df_copy.columns: df_copy.drop(columns=['merge_date_key'], inplace=True, errors='ignore')
        return df_copy

    def _add_quantile_buckets(self, df):
        """
        Adds quantile-based buckets for configured difference metrics.
        """
        df_copy = df.copy()
        for diff_col_name, bucket_col_name in self.metrics_to_bucket_map.items():
            if diff_col_name not in df_copy.columns:
                print(f"Warning: Source column '{diff_col_name}' not found for bucketing. Skipping bucket '{bucket_col_name}'.")
                df_copy[bucket_col_name] = pd.NA
                continue
            
            numeric_source_col = pd.to_numeric(df_copy[diff_col_name], errors='coerce')
            non_na_count = numeric_source_col.count()

            if non_na_count == 0 : # No data to bucket
                 df_copy[bucket_col_name] = pd.NA
                 continue
            if non_na_count < self.n_buckets and non_na_count > 0 :
                print(f"Warning: Not enough unique non-NA values in '{diff_col_name}' ({numeric_source_col.nunique()}) to create {self.n_buckets} distinct buckets for '{bucket_col_name}'. Resulting buckets may be fewer or pd.cut will be used.")
                # Fallback to pd.cut if qcut is problematic due to too few unique values for N buckets
                try:
                    df_copy[bucket_col_name] = pd.cut(
                        numeric_source_col,
                        bins=self.n_buckets, # Let pandas determine bin edges
                        labels=self.bucket_labels if len(self.bucket_labels) == self.n_buckets else False, # Ensure labels match bins
                        duplicates='drop',
                        include_lowest=True
                    )
                except Exception as e_cut:
                    print(f"Error with pd.cut for '{diff_col_name}': {e_cut}. Assigning NA to '{bucket_col_name}'.")
                    df_copy[bucket_col_name] = pd.NA
                continue

            try:
                df_copy[bucket_col_name] = pd.qcut(
                    numeric_source_col,
                    q=self.n_buckets,
                    labels=self.bucket_labels,
                    duplicates='drop'
                )
            except Exception as e_qcut:
                print(f"Error creating quantile buckets for '{diff_col_name}' with pd.qcut: {e_qcut}. Assigning NA to '{bucket_col_name}'.")
                df_copy[bucket_col_name] = pd.NA
        return df_copy

    def merge_data(self, ga4_df, wp_df):
        """Merges GA4 and WordPress dataframes."""
        if 'pagepath' not in ga4_df.columns or ga4_df['pagepath'].isnull().all():
            print("Error: 'pagepath' column is missing or all null in GA4 data. Cannot merge effectively.")
            # Return WordPress data if GA4 is unusable for merge, or empty if both problematic
            return wp_df if not wp_df.empty else pd.DataFrame()
        if 'pagepath' not in wp_df.columns or wp_df['pagepath'].isnull().all():
            print("Error: 'pagepath' column is missing or all null in WP data. Cannot merge effectively.")
            # Return GA4 data if WP is unusable for merge
            return ga4_df if not ga4_df.empty else pd.DataFrame()

        # Perform the merge
        merged_df = pd.merge(wp_df, ga4_df, on='pagepath', how='left')
        
        return merged_df

    def select_and_rename_columns(self, merged_df):
        """Selects and renames columns as per configuration."""
        if merged_df.empty:
            return pd.DataFrame()

        # Ensure all columns to keep exist in the merged_df, fill with NaN/NA if not
        # print(f"Columns in merged_df before selection: {merged_df.columns.tolist()}")
        current_columns = merged_df.columns.tolist()
        columns_to_actually_select = []
        for col in self.columns_to_keep:
            if col not in current_columns:
                merged_df[col] = pd.NA # Use pd.NA for potentially categorical/object types
                print(f"Warning: Column '{col}' specified in COLUMNS_TO_KEEP not found in merged data. Added as empty (NA) column.")
            columns_to_actually_select.append(col)
        
        # Select only the specified columns
        final_df = merged_df[columns_to_actually_select]
        # print(f"Columns in final_df after selection: {final_df.columns.tolist()}")
        return final_df

    def transform_data(self, ga4_df, wp_df):
        """Main transformation pipeline."""
        cleaned_ga4_df = self._clean_ga4_data(ga4_df.copy() if ga4_df is not None else pd.DataFrame())
        cleaned_wp_df = self._clean_wp_data(wp_df.copy() if wp_df is not None else pd.DataFrame())
        
        if cleaned_ga4_df.empty and cleaned_wp_df.empty:
            print("Both GA4 and WordPress input data are empty after cleaning. Returning empty DataFrame.")
            # Ensure all expected columns (diffs and buckets) are present if COLUMNS_TO_KEEP expects them
            # This part is tricky as we don't have pubdate to begin with.
            # select_and_rename_columns will add them as NA if they are in COLUMNS_TO_KEEP.
            return self.select_and_rename_columns(pd.DataFrame())


        merged_df = self.merge_data(cleaned_ga4_df, cleaned_wp_df)
        
        if merged_df.empty:
            print("Merge resulted in an empty DataFrame. Check merge keys and data integrity.")
            # select_and_rename_columns will handle adding missing COLUMNS_TO_KEEP as NA
            return self.select_and_rename_columns(pd.DataFrame())
        
        pubdate_column_name = 'pubdate'
        
        # Add benchmark difference columns and quantile bucket columns
        merged_df_with_calcs = self._add_benchmark_differences(merged_df, pubdate_column_name)
        transformed_df_final = self._add_quantile_buckets(merged_df_with_calcs)

        final_df_selected = self.select_and_rename_columns(transformed_df_final)
        
        return final_df_selected

# Example usage (for testing)
if __name__ == '__main__':
    # Create dummy data for GA4
    ga4_dummy_data = {
        'Percorso pagina e classe schermata': ['/page1/', '/page2/', '/page3/path/', '/page1/'],
        'Visualizzazioni': [100, 200, 50, 120],
        'Utenti attivi': [10, 20, 5, 12],
        'Durata media del coinvolgimento per utente attivo': [60.0, 120.0, 30.0, 70.0],
        'Conteggio eventi': [1000, 2000, 500, 1100]
    }
    ga4_test_df = pd.DataFrame(ga4_dummy_data)

    # Create dummy data for WordPress
    wp_dummy_data = {
        'title': ['Post 1', 'Post 2', 'Post 4', 'Post 1 Again'],
        'link': ['http://example.com/page1', 'https://example.com/page2/', 'http://example.com/page4/', 'http://example.com/page1'], # Duplicate link for testing merge
        'category': ['News', 'Tech', 'News', 'Updates'],
        'pubdate': ['2023-01-01 10:00:00', '2023-01-02 11:00:00', '2023-01-03 12:00:00', '2023-01-01 14:00:00'], # Same date for page1
        '_yoast_wpseo_focuskw': ['kw1', 'kw2', 'kw4', 'kw1-update'],
        '_yoast_wpseo_metadesc': ['desc1', 'desc2', 'desc4', 'desc1-update'],
        '_yoast_wpseo_linkdex': [70, 80, 60, 75],
        'content': ['Content 1', 'Content 2', 'Content 4', 'Content 1 Updated']
    }
    wp_test_df = pd.DataFrame(wp_dummy_data)
    
    # Add another entry for a different date to test median benchmark
    wp_dummy_data_more = {
        'title': ['Post 5 Jan 2'], 'link': ['http://example.com/page5'], 'category': ['News'], 
        'pubdate': ['2023-01-02 15:00:00'], '_yoast_wpseo_linkdex': [50], 'content': ['Content 5']
    }
    ga4_dummy_data_more = {
        'Percorso pagina e classe schermata': ['/page5/'], 'Visualizzazioni': [300], 
        'Utenti attivi': [30], 'Durata media del coinvolgimento per utente attivo': [150.0]
    }
    wp_test_df = pd.concat([wp_test_df, pd.DataFrame(wp_dummy_data_more)], ignore_index=True)
    ga4_test_df = pd.concat([ga4_test_df, pd.DataFrame(ga4_dummy_data_more)], ignore_index=True)


    example_config = {
        "COLUMNS_TO_KEEP": [
            "title", "link", "category", "pagepath", "pubdate", "views", "active users",
            "average engagement time per active user",
            "_yoast_wpseo_focuskw", "_yoast_wpseo_metadesc", "_yoast_wpseo_linkdex", "content",
            "diff_with_daily_benchmark_views", "views_bucket",
            "diff_with_daily_benchmark_active_users", "active_users_bucket",
            "diff_with_daily_benchmark_average_engagement_time_per_active_user", "average_engagement_time_per_active_user_bucket"
        ],
        "BUCKET_LABELS": ["Molto Basso", "Basso", "Medio", "Alto", "Molto Alto"],
        "N_BUCKETS": 5,
        "METRICS_FOR_BENCHMARK": { # Original Name: Base Name for derived cols
            "views": "views",
            "active users": "active_users",
            "average engagement time per active user": "average_engagement_time_per_active_user"
        }
        # METRICS_TO_BUCKET_MAP will be derived in __init__
    }
    
    transformer = Transformer(config=example_config)
    transformed_df = transformer.transform_data(ga4_test_df, wp_test_df)
    
    print("\n--- Transformed Data ---")
    print(transformed_df.head(10))
    print(f"\nTransformed Data Shape: {transformed_df.shape}")
    print("\nTransformed Data Columns:")
    print(transformed_df.columns.tolist())
    print("\nTransformed Data Info:")
    transformed_df.info()

    print("\n--- Checking some values ---")
    if not transformed_df.empty:
        for col in ["views_bucket", "active_users_bucket", "average_engagement_time_per_active_user_bucket"]:
            if col in transformed_df.columns:
                print(f"\nValue counts for {col}:")
                print(transformed_df[col].value_counts(dropna=False))