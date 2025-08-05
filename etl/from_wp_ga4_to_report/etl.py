from .extractor import Extractor
from .transformer import Transformer
from .loader import Loader
# import config as etl_config # Removed direct import of config module

class EtlPipeline:
    def __init__(self, config):
        """
        Initializes the ETL pipeline with a configuration dictionary.
        The config dictionary should contain all necessary paths and parameters.
        Example keys: "GA4_FILE_PATH", "WP_FILE_PATH", "WP_FILE_TYPE",
                      "OUTPUT_FILE_PATH", "COLUMNS_TO_KEEP",
                      "LOAD_KWARGS" (optional, for loader.load_data).
        """
        self.config = config
        self.extractor = Extractor(config)
        self.transformer = Transformer(config)
        self.loader = Loader(config)

    def run(self):
        """
        Executes the full ETL pipeline.
        """
        print("Starting ETL pipeline...")

        # 1. Extract data
        print("Step 1: Extracting data...")
        try:
            ga4_data, wp_data = self.extractor.extract_all_data()
            if ga4_data.empty:
                print("Warning: GA4 data is empty after extraction.")
            else:
                print(f"GA4 data extracted. Shape: {ga4_data.shape}")
                print(f"GA4 data columns: {ga4_data.columns.tolist()}")
            if wp_data.empty:
                print("Warning: WordPress data is empty after extraction.")
            else:
                print(f"WordPress data extracted. Shape: {wp_data.shape}")
                print(f"WordPress data columns: {wp_data.columns.tolist()}")
        except Exception as e:
            print(f"Error during extraction: {e}")
            return

        # 2. Transform data
        print("\nStep 2: Transforming data...")
        try:
            transformed_data = self.transformer.transform_data(ga4_data, wp_data)
            if transformed_data.empty and not (ga4_data.empty and wp_data.empty):
                print("Warning: Transformed data is empty, but input data was present. Check transformation logic and merge keys.")
            elif transformed_data.empty:
                print("Transformed data is empty.")
            else:
                print(f"Data transformed successfully. Shape: {transformed_data.shape}")
                print(f"Transformed data columns: {transformed_data.columns.tolist()}")
        except Exception as e:
            print(f"Error during transformation: {e}")
            return

        # 3. Load data
        print("\nStep 3: Loading data...")
        try:
            # Get loader specific arguments from config if provided
            load_kwargs = self.config.get("LOAD_KWARGS", {}) 
            self.loader.load_data(transformed_data, **load_kwargs)
            # Example: if you want to use the static save method from Loader
            # if not transformed_data.empty:
            #     Loader.save(transformed_data, self.config["OUTPUT_FILE_PATH"], self.config["COLUMNS_TO_KEEP"])
            # else:
            #     print("Skipping load step as transformed data is empty.")

        except Exception as e:
            print(f"Error during loading: {e}")
            return
        
        print("\nETL pipeline finished successfully.")

# Example of how to run the pipeline
if __name__ == '__main__':
    # This configuration would typically be loaded from a file,
    # environment variables, or defined in a higher-level script (like a notebook).
    pipeline_config = {
        "GA4_FILE_PATH": "../../data/010525_300525_Pagine_e_schermate_Percorso_pagina_e_classe_schermata.csv",
        "WP_FILE_PATH": "../../data/taxidriversit.WordPress.2025-05-30.xml",
        "WP_FILE_TYPE": "xml", # or "csv"
        "OUTPUT_FILE_PATH": "../../output/processed_data_pipeline.csv",
        "COLUMNS_TO_KEEP": [
            "title", "link", "category", "pagepath", "pubdate", "views",
            "active users", "views per active user", "average engagement time per active user",
            "_yoast_wpseo_focuskw", "_yoast_wpseo_metadesc", "_yoast_wpseo_linkdex", "content"
            # Add other GA4 columns you want to keep if they are in the GA4 export and renamed in transformer
        ],
        "LOAD_KWARGS": { # Optional: arguments for the loader's to_csv method
            "sep": ",",
            "encoding": "utf-8"
        }
    }

    # For testing with dummy data paths if real files are not present or for quick tests
    # Ensure dummy files exist at these paths if you uncomment this
    # pipeline_config_test = {
    #     "GA4_FILE_PATH": "../../data/dummy_ga4.csv", # Create this file for testing
    #     "WP_FILE_PATH": "../../data/dummy_wp.xml",   # Create this file for testing
    #     "WP_FILE_TYPE": "xml",
    #     "OUTPUT_FILE_PATH": "../../output/dummy_processed_data.csv",
    #     "COLUMNS_TO_KEEP": ["title", "link", "views", "pagepath"],
    #     "LOAD_KWARGS": {"sep": ";"}
    # }
    
    # Create and run the pipeline
    # etl_process = EtlPipeline(config=pipeline_config_test) # For dummy data test
    etl_process = EtlPipeline(config=pipeline_config)
    etl_process.run()

