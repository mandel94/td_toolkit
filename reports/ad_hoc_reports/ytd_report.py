import os
import sys
import pandas as pd


from typing import List

# Category mapping: Maps broader category names to a set of specific path keywords.
CATEGORIES = {
    "News": {"latest-news", "focus-italia"},
    # "Anticipazioni": {"anticipazioni"},
    "Recensioni": {
        "review",
        "netflix-film",
        "sky-film",
        "disney-film",
        "mubi",
        "mubi-film",
        "live-streaming-on-demand",
        "approfondimenti",
        "streaming",
    },
    "In Sala": {"in-sala"},
    "Animazione": {"animazione"},
    "Approfondimento": {"approfondimento"},
    "Festival di Cinema": {"festival-di-cinema"},
    "Trailers": {"trailers"},
    "Serie TV": {
        "serie-tv",
        "netflix-serie-tv",
        "prime-video-serietv",
        "sky-serie-tv",
        "disney-serietv",
        "paramount-serie-tv",
        "appletv-serietv",
        "tim-vision-serie-tv",
    },
    "Guide e Film da Vedere": {"film-da-vedere"},
    "Speciali e Magazine": {"magazine-2", "taxidrivers-magazine"},
    "Rubriche": {"rubriche"},
    "Interviste": {"interviews"},
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


def run_report_etl_and_analysis():
    """
    Runs the ETL pipeline, processes the data, maps categories, filters links, and saves the full DataFrame to an Excel file.
    """
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root_from_script = os.path.abspath(
        os.path.join(current_script_dir, "..", "..")
    )  # Adjust as needed

    etl_pipeline_config = {
        "GA4_FILE_PATH": os.path.join(
            project_root_from_script,
            "data",
            "YTD_21 maggio 2025_Pagine_e_schermate_Percorso_pagina_e_classe_schermata (1).csv",
        ),
        "WP_FILE_PATH": os.path.join(
            project_root_from_script,
            "data",
            "YTD_21 maggio 2025_taxidriversit.WordPress.2025-05-21.xml",
        ),
        "WP_FILE_TYPE": "xml",
        "OUTPUT_FILE_PATH": os.path.join(
            project_root_from_script, "output", "ytd_report_21052025.csv"
        ),
        "COLUMNS_TO_KEEP": [
            "title",
            "link",
            "category",
            "pagepath",
            "pubdate",
            "views",
            "active users",
            "views per active user",
            "average engagement time per active user",
            "_yoast_wpseo_focuskw",
            "_yoast_wpseo_metadesc",
            "_yoast_wpseo_linkdex",  # "content"
            # Columns from benchmark and bucketing
            "diff_with_daily_benchmark_views",
            "diff_with_daily_benchmark_active_users",
            "diff_with_daily_benchmark_average_engagement_time_per_active_user",
            "views_bucket",
            "active_users_bucket",
            "average_engagement_time_per_active_user_bucket",
        ],
        "LOAD_KWARGS": {"sep": ",", "encoding": "utf-8"},
        # Transformer specific configurations
        "BUCKET_LABELS": ["Molto Basso", "Basso", "Medio", "Alto", "Molto Alto"],
        "N_BUCKETS": 5,
        "METRICS_FOR_BENCHMARK": {
            "views": "views",  # Original metric name : base name for derived columns
            "active users": "active_users",
            "average engagement time per active user": "average_engagement_time_per_active_user",
        },
        # METRICS_TO_BUCKET_MAP will be derived by the Transformer based on METRICS_FOR_BENCHMARK
    }

    try:
        # Ensure the import path matches your directory structure from the project root
        # from etl.from_wp_ga4_to_report.etl import EtlPipeline
        # Add project root to sys.path
        if project_root_from_script not in sys.path:
            sys.path.append(project_root_from_script)
            print(f"Added to sys.path: {project_root_from_script}")  # For debugging
        from etl.from_wp_ga4_to_report.etl import EtlPipeline

        print("Running ETL Pipeline for Report...")
        etl_process = EtlPipeline(config=etl_pipeline_config)
        etl_process.run()
        print("ETL Pipeline for Report finished.")
    except ImportError as e_import:
        print(f"ImportError: {e_import}")
        print(
            "Error: Could not import EtlPipeline. Ensure 'etl/from_wp_ga4_to_report/etl.py' exists and that"
        )
        print(
            "'etl/' and 'etl/from_wp_ga4_to_report/' directories contain an '__init__.py' file."
        )
        print(f"Current sys.path: {sys.path}")
        return
    except Exception as e:
        print(f"An error occurred during the ETL process: {e}")
        return

    # Load Processed Data
    processed_csv_path = etl_pipeline_config["OUTPUT_FILE_PATH"]

    df = pd.DataFrame()
    try:
        df = pd.read_csv(
            processed_csv_path,
            sep=etl_pipeline_config.get("LOAD_KWARGS", {}).get("sep", ","),
        )
        print(f"Successfully loaded {processed_csv_path}")
        print(f"DataFrame shape: {df.shape}")
    except FileNotFoundError:
        print(
            f"ERROR: The file {processed_csv_path} was not found. Ensure the ETL pipeline ran successfully."
        )
        return
    except Exception as e:
        print(f"An error occurred while loading the data: {e}")
        return

    if df.empty:
        print("Processed DataFrame is empty. Skipping analysis.")
        return

    # Filter out rows where 'link' ends with p={number}
    import re

    if "link" in df.columns:
        pattern = re.compile(r"p=\d+$")
        before_count = len(df)
        df = df[~df["link"].astype(str).str.contains(pattern)]
        after_count = len(df)
        print(
            f"Filtered out rows with links ending in 'p={{number}}': {before_count - after_count} rows removed."
        )
    else:
        print("Warning: 'link' column not found in DataFrame. Skipping link filter.")

    # Map the 'category' column using the map_categories function
    if "category" in df.columns:
        print(
            "Mapping 'category' column to content categories using map_categories function..."
        )
        df["category"] = df["link"].astype(str).apply(map_categories)
    else:
        print(
            "Warning: 'category' column not found in DataFrame. Skipping category mapping."
        )

    # Save all articles (no bucket filter)
    output_dir = os.path.dirname(processed_csv_path)
    all_output_path = os.path.join(output_dir, "td_ytd_report_21052025.csv")
    try:
        # save output as csv file using tab separator and italian encoding
        # Remove em-dashes and en-dashes, replace with hyphens, and other special characters
        df = df.replace(
            {
                "\u2014": "-",  # em dash
                "\u2013": "-",  # en dash
                "\u2019": "'",  # right single quote
                "\u2018": "'",  # left single quote
                "\u201c": '"',  # left double quote
                "\u201d": '"',  # right double quote
                "\u2026": ".",  # ellipsis
                "\u00a0": " ",  # non-breaking space
                "\u00b7": "-",  # middle dot
                "\u2122": "",  # trademark
                "\u0161": "s",  # Latin small letter s with caron
                "\u00e9": "e",  # é
                "\u00e0": "a",  # à
                "\u00f2": "o",  # ò
                "\u00e8": "e",  # è
                "\u00f9": "u",  # ù
                "\u00ec": "i",  # ì
                "\u00c0": "A",  # À
                "\u00c8": "E",  # È
                "\u00c9": "E",  # É
                "\u00cc": "I",  # Ì
                "\u00d2": "O",  # Ò
                "\u00d9": "U",  # Ù
                "\u20ac": "EUR",  # Euro sign
                "\u00e2": "a",  # â
                "\u00f4": "o",  # ô
                "\u014d": "o",
                "\u0219": "s",  # Latin small letter s with comma below
                "\u0116": "e"  # Latin capital letter E with macron
            },
            regex=True,
        )
        df.to_csv(all_output_path, sep=",", encoding="utf-8", index=False)
        print(f"All articles saved to {all_output_path}")
    except Exception as e:
        print(f"Error saving data to CSV: {e}")
    return df


if __name__ == "__main__":
    run_report_etl_and_analysis()
    run_report_etl_and_analysis()
