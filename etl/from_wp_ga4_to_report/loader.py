import pandas as pd
import os

class Loader:
    def __init__(self, config):
        """
        Initializes the Loader with a configuration dictionary.
        Args:
            config (dict): A dictionary containing 'OUTPUT_FILE_PATH'.
        """
        self.output_file_path = config['OUTPUT_FILE_PATH']

    def load_data(self, df, **kwargs):
        """
        Saves the DataFrame to a CSV file.
        Args:
            df (pd.DataFrame): The DataFrame to save.
            **kwargs: Additional keyword arguments to pass to pandas.DataFrame.to_csv().
                      Example: sep=',', encoding='utf-8'.
        """
        if df.empty:
            print("DataFrame is empty. Nothing to load.")
            return

        try:
            # Ensure the output directory exists
            output_dir = os.path.dirname(self.output_file_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)
                print(f"Created directory: {output_dir}")

            # Default to no index and utf-8 encoding if not specified
            kwargs.setdefault('index', False)
            kwargs.setdefault('encoding', 'utf-8')
            
            df.to_csv(self.output_file_path, **kwargs)
            print(f"Data successfully loaded to {self.output_file_path}")
        except Exception as e:
            print(f"Error loading data to CSV: {e}")

    @staticmethod
    def save(df: pd.DataFrame, output_path: str, columns_to_keep: list = None, file_format: str = 'csv', compression: str = None) -> None:
        """
        Save DataFrame to disk in the desired format.

        Args:
            df (pd.DataFrame): The data to save.
            output_path (str): Full file path including the filename.
            columns_to_keep (list): Optional list of columns to save.
            file_format (str): Output format ('csv', 'parquet', 'json').
            compression (str): Optional compression method ('gzip', 'brotli', etc).

        Raises:
            RuntimeError: If saving fails.
        """
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            if columns_to_keep:
                missing_columns = set(columns_to_keep) - set(df.columns)
                if missing_columns:
                    raise ValueError(f"Columns not found in DataFrame: {missing_columns}")
                df = df[columns_to_keep]

            if file_format.lower() == 'csv':
                df.to_csv(output_path, index=False, encoding='utf-8', compression=compression)
            elif file_format.lower() == 'parquet':
                df.to_parquet(output_path, index=False, compression=compression or 'snappy')
            elif file_format.lower() == 'json':
                df.to_json(output_path, orient='records', lines=True)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")

            print(f"Data successfully saved to {output_path}")

        except Exception as e:
            raise RuntimeError(f"Failed to save data: {e}\n{traceback.format_exc()}")

    @staticmethod
    def save_metadata(metadata: dict, output_path: str) -> None:
        """
        Save metadata dictionary to a JSON file.

        Args:
            metadata (dict): Metadata information.
            output_path (str): Full file path for the metadata JSON.

        Raises:
            RuntimeError: If saving fails.
        """
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=4, ensure_ascii=False)
            print(f"Metadata successfully saved to {output_path}")
        except Exception as e:
            raise RuntimeError(f"Failed to save metadata: {e}\n{traceback.format_exc()}")

    @staticmethod
    def log_summary(df: pd.DataFrame) -> None:
        """
        Print a simple summary of the dataframe.

        Args:
            df (pd.DataFrame): DataFrame to summarize.
        """
        try:
            print("Data Summary:")
            print(f"Shape: {df.shape}")
            print(f"Columns: {df.columns.tolist()}")
            print(df.dtypes)
            print(df.head(3))
        except Exception as e:
            print(f"Failed to print summary: {e}")

# Example usage (for testing)
if __name__ == '__main__':
    # Create a dummy DataFrame
    dummy_data = {
        'title': ['Test Post 1', 'Test Post 2'],
        'link': ['/test-post-1', '/test-post-2'],
        'views': [100, 150]
    }
    test_df = pd.DataFrame(dummy_data)

    # Define an example configuration for the loader
    example_config = {
        "OUTPUT_FILE_PATH": "../../output/test_processed_data.csv"
    }
    
    loader = Loader(config=example_config)
    
    # Load the data (default separator is comma)
    loader.load_data(test_df)
    
    # Example with a different separator
    # loader.load_data(test_df, sep=';')

    # Verify file creation (optional)
    if os.path.exists(example_config["OUTPUT_FILE_PATH"]):
        print(f"File {example_config['OUTPUT_FILE_PATH']} created successfully.")
        # print(pd.read_csv(example_config["OUTPUT_FILE_PATH"])) # to check content
    else:
        print(f"File {example_config['OUTPUT_FILE_PATH']} was not created.")

