import os
import sys
import pandas as pd


from typing import List

# Category mapping: Maps broader category names to a set of specific path keywords.
CATEGORIES = {
    "News": {"latest-news", "focus-italia"},
    # "Anticipazioni": {"anticipazioni"},
    "Recensioni": {"review", "netflix-film", "sky-film", "disney-film", "mubi", "mubi-film",
                   "live-streaming-on-demand", "approfondimenti", "streaming"},
    "In Sala": {"in-sala"},
    "Animazione": {"animazione"},
    "Approfondimento": {"approfondimento"},
    "Festival di Cinema": {"festival-di-cinema"},
    "Trailers": {"trailers"},
    "Serie TV": {"serie-tv", "netflix-serie-tv", "prime-video-serietv", "sky-serie-tv",
                 "disney-serietv", "paramount-serie-tv", "appletv-serietv", "tim-vision-serie-tv"},
    "Guide e Film da Vedere": {"film-da-vedere"},
    "Speciali e Magazine": {"magazine-2", "taxidrivers-magazine"},
    "Rubriche": {"rubriche"},
    "Interviste": {"interviews"}
}

def map_categories(path: str) -> str:
    """
    Maps a given URL path to a corresponding content category based on predefined keywords.

    :param path: The URL path string to categorize. Example: ``"recensioni/netflix-film/in-sala"``
    :type path: str

    :returns: The mapped category name. Possible values include:
              - "Recensioni"
              - "News"
              - "Serie TV"
              - "Recensioni / In Sala"
              - "Trailers / In Sala"
              - "Altro" (default if no match is found)
    :rtype: str

    The function analyzes the path by splitting it into subpaths and checking for intersections
    with keyword sets defined in the global ``CATEGORIES`` dictionary.
    """
    subpaths = path.split("/")
    
    if not subpaths:
        return "Altro"

    for category, keywords in CATEGORIES.items():
        if keywords.intersection(subpaths):
            # Special case: category mixed with "in-sala"
            # If anticipazioni is in the path, it should be prioritized
            if "anticipazioni" in subpaths:
                return "Anticipazioni"
            if category == "Recensioni" and "in-sala" in path:
                return "Recensioni / In Sala"
            if category == "Trailers" and "in-sala" in path:
                return "Trailers / In Sala"
            return category

    return "Altro"


def run_report_etl_and_analysis(top_only: bool = False, report_prefix: str = ""):
    """
    Runs the ETL pipeline, identifies top and flop articles based on engagement buckets
    and benchmark differences, and saves them to an Excel file.
    If top_only is True, only the top 10 articles are saved; otherwise, all relevant rows are saved.
    The report_prefix argument is prepended to output filenames to avoid overwriting files from other reports.
    """
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root_from_script = os.path.abspath(os.path.join(current_script_dir, "..")) # Adjust as needed


    etl_pipeline_config = {
        "GA4_FILE_PATH": os.path.join(project_root_from_script, "data", "010525_300525_Pagine_e_schermate_Percorso_pagina_e_classe_schermata.csv"),
        "WP_FILE_PATH": os.path.join(project_root_from_script, "data", "taxidriversit.WordPress.2025-05-30.xml"),
        "WP_FILE_TYPE": "xml",
        "OUTPUT_FILE_PATH": os.path.join(project_root_from_script, "output", "processed_data_for_report.csv"),
        "COLUMNS_TO_KEEP": [
            "title", "link", "category", "pagepath", "pubdate", "views",
            "active users", "views per active user", "average engagement time per active user",
            "_yoast_wpseo_focuskw", "_yoast_wpseo_metadesc", "_yoast_wpseo_linkdex", #"content"
            # Columns from benchmark and bucketing
            "diff_with_daily_benchmark_views",
            "diff_with_daily_benchmark_active_users",
            "diff_with_daily_benchmark_average_engagement_time_per_active_user",
            "views_bucket",
            "active_users_bucket",
            "average_engagement_time_per_active_user_bucket"
        ],
        "LOAD_KWARGS": {
            "sep": ",",
            "encoding": "utf-8"
        },
        # Transformer specific configurations
        "BUCKET_LABELS": ["Molto Basso", "Basso", "Medio", "Alto", "Molto Alto"],
        "N_BUCKETS": 5,
        "METRICS_FOR_BENCHMARK": {
            "views": "views", # Original metric name : base name for derived columns
            "active users": "active_users",
            "average engagement time per active user": "average_engagement_time_per_active_user"
        }
        # METRICS_TO_BUCKET_MAP will be derived by the Transformer based on METRICS_FOR_BENCHMARK
    }
 
    try:
        # Ensure the import path matches your directory structure from the project root
        # from etl.from_wp_ga4_to_report.etl import EtlPipeline
               # Add project root to sys.path
        if project_root_from_script not in sys.path:
            sys.path.append(project_root_from_script)
            print(f"Added to sys.path: {project_root_from_script}") # For debugging
        from etl.from_wp_ga4_to_report.etl import EtlPipeline
        print("Running ETL Pipeline for Report...")
        etl_process = EtlPipeline(config=etl_pipeline_config)
        etl_process.run()
        print("ETL Pipeline for Report finished.")
    except ImportError as e_import:
        print(f"ImportError: {e_import}")
        print("Error: Could not import EtlPipeline. Ensure 'etl/from_wp_ga4_to_report/etl.py' exists and that")
        print("'etl/' and 'etl/from_wp_ga4_to_report/' directories contain an '__init__.py' file.")
        print(f"Current sys.path: {sys.path}")
        return
    except Exception as e:
        print(f"An error occurred during the ETL process: {e}")
        return

    # Load Processed Data
    processed_csv_path = etl_pipeline_config["OUTPUT_FILE_PATH"]
    
    df = pd.DataFrame()
    try:
        df = pd.read_csv(processed_csv_path, sep=etl_pipeline_config.get("LOAD_KWARGS", {}).get("sep", ","))
        print(f"Successfully loaded {processed_csv_path}")
        print(f"DataFrame shape: {df.shape}")
    except FileNotFoundError:
        print(f"ERROR: The file {processed_csv_path} was not found. Ensure the ETL pipeline ran successfully.")
        return
    except Exception as e:
        print(f"An error occurred while loading the data: {e}")
        return

    if df.empty:
        print("Processed DataFrame is empty. Skipping top/flop analysis.")
        return

    # Filter out rows where 'link' ends with p={number}
    import re
    if 'link' in df.columns:
        pattern = re.compile(r'p=\d+$')
        before_count = len(df)
        df = df[~df['link'].astype(str).str.contains(pattern)]
        after_count = len(df)
        print(f"Filtered out rows with links ending in 'p={{number}}': {before_count - after_count} rows removed.")
    else:
        print("Warning: 'link' column not found in DataFrame. Skipping link filter.")

    # Map the 'category' column using the map_categories function
    if 'category' in df.columns:
        print("Mapping 'category' column to content categories using map_categories function...")
        df['category'] = df['link'].astype(str).apply(map_categories)
    else:
        print("Warning: 'category' column not found in DataFrame. Skipping category mapping.")

    # Define key columns for ranking
    engagement_bucket_col = "average_engagement_time_per_active_user_bucket"
    views_diff_col = "diff_with_daily_benchmark_views"
    raw_views_col = "views" # Fallback column

    # Check if necessary columns for new ranking exist
    if engagement_bucket_col not in df.columns or views_diff_col not in df.columns:
        print(f"Warning: Required columns for ranking ('{engagement_bucket_col}', '{views_diff_col}') not found in DataFrame.")
        print(f"Available columns: {df.columns.tolist()}")
        print(f"Falling back to ranking by raw '{raw_views_col}'.")
        if raw_views_col not in df.columns:
            print(f"Error: Fallback column '{raw_views_col}' also not found. Cannot determine top/flop articles.")
            return
        df[raw_views_col] = pd.to_numeric(df[raw_views_col], errors='coerce')
        df.dropna(subset=[raw_views_col], inplace=True)
        top_10_articles = df.sort_values(by=raw_views_col, ascending=False).head(10)
        flop_10_articles = df.sort_values(by=raw_views_col, ascending=True).head(10)
    else:
        print(f"Ranking articles based on '{engagement_bucket_col}' and '{views_diff_col}'.")
        df[views_diff_col] = pd.to_numeric(df[views_diff_col], errors='coerce')
        
        # Ensure bucket column is treated as categorical for proper sorting if needed, though direct string comparison works for these labels
        bucket_order = ["Molto Basso", "Basso", "Medio", "Alto", "Molto Alto"]
        if pd.api.types.is_categorical_dtype(df[engagement_bucket_col]):
            print(f"'{engagement_bucket_col}' is already categorical.")
            df[engagement_bucket_col] = df[engagement_bucket_col].cat.set_categories(bucket_order, ordered=True)
        else:
            print(f"Converting '{engagement_bucket_col}' to categorical with order: {bucket_order}")
            # Convert to categorical with specified order
            df[engagement_bucket_col] = pd.Categorical(df[engagement_bucket_col], categories=bucket_order, ordered=True)


        # Identify Top 10 Articles
        # Articles in "Alto" or "Molto Alto" engagement, then highest views_diff
        top_articles_df = df[df[engagement_bucket_col].isin(["Alto", "Molto Alto"])].copy()
        if not top_articles_df.empty:
            print(f"Found {len(top_articles_df)} articles in 'Alto' or 'Molto Alto' engagement buckets.")
            print("Sorting top articles by engagement bucket and views difference")
            print("Taking the top 10 articles...")
            print("Done.")
            print(f"Top articles: {top_articles_df}")
            top_10_articles = top_articles_df.sort_values(by=[engagement_bucket_col, views_diff_col], ascending=[False, False]).head(10)
        else:
            print("No articles found in 'Alto' or 'Molto Alto' engagement buckets. Selecting top by views difference globally.")
            top_10_articles = df.sort_values(by=views_diff_col, ascending=False).head(10)
        
        # Identify Flop 10 Articles
        # Articles in "Basso" or "Molto Basso" engagement, then lowest views_diff
        flop_articles_df = df[df[engagement_bucket_col].isin(["Basso", "Molto Basso"])].copy()
        if not flop_articles_df.empty:
            print(f"Found {len(flop_articles_df)} articles in 'Basso' or 'Molto Basso' engagement buckets.")
            print("Sorting flop articles by engagement bucket and views difference")
            print("Taking the flop 10 articles...")
            flop_10_articles = flop_articles_df.sort_values(by=[engagement_bucket_col, views_diff_col], ascending=[True, True]).head(10)
            print("Done.")
            print(f"Flop articles: {flop_articles_df}")
        else:
            print("No articles found in 'Basso' or 'Molto Basso' engagement buckets. Selecting flop by views difference globally.")
            flop_10_articles = df.sort_values(by=views_diff_col, ascending=True).head(10)

    # Save to separate Excel files
    output_dir = os.path.dirname(processed_csv_path)
    prefix = f"{report_prefix}_" if report_prefix else ""
    top_output_path = os.path.join(output_dir, f"{prefix}td_report_top.xlsx")
    flop_output_path = os.path.join(output_dir, f"{prefix}td_report_flop.xlsx")
    all_output_path = os.path.join(output_dir, f"{prefix}td_report_all_articles.xlsx")
    try:
        # Save Top Articles
        if not top_10_articles.empty:
            if top_only:
                top_10_articles.to_excel(top_output_path, index=False)
            else:
                if 'top_articles_df' in locals() and not top_articles_df.empty:
                    top_articles_df.to_excel(top_output_path, index=False)
                else:
                    top_10_articles.to_excel(top_output_path, index=False)
        else:
            pd.DataFrame([{"message": "No top articles identified with current criteria."}]).to_excel(top_output_path, index=False)

        # Save Flop Articles
        if not flop_10_articles.empty:
            if top_only:
                flop_10_articles.to_excel(flop_output_path, index=False)
            else:
                if 'flop_articles_df' in locals() and not flop_articles_df.empty:
                    flop_articles_df.to_excel(flop_output_path, index=False)
                else:
                    flop_10_articles.to_excel(flop_output_path, index=False)
        else:
            pd.DataFrame([{"message": "No flop articles identified with current criteria."}]).to_excel(flop_output_path, index=False)

        # Save All Articles (no bucket filter)
        df.to_excel(all_output_path, index=False)

        print(f"Top articles saved to {top_output_path}")
        print(f"Flop articles saved to {flop_output_path}")
        print(f"All articles saved to {all_output_path}")
    except Exception as e:
        print(f"Error saving data to Excel: {e}")

if __name__ == "__main__":
    # Ensure openpyxl is installed
    # try:
    #     import openpyxl
    # except ImportError:
    #     print("openpyxl library is not installed. Please install it by running: pip install openpyxl")
    #     sys.exit(1) # Exit if openpyxl is not available
    
    user_input = input("Return only top 10 articles? (y/n): ").strip().lower()
    top_only = user_input == 'y'
    report_prefix = input("Enter a prefix for the report output files (leave blank for none): ").strip()
    run_report_etl_and_analysis(top_only=top_only, report_prefix=report_prefix)
    run_report_etl_and_analysis(top_only=top_only, report_prefix=report_prefix)

