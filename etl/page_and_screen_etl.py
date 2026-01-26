import pandas as pd
import os
from abc import ABC, abstractmethod

class BasePageAndScreenETL(ABC):
    def __init__(self, input_filename=None, output_filename=None, df=None):
        self.input_filename = input_filename
        self.output_filename = output_filename or (f"out_{input_filename}" if input_filename else None)
        self.data_path = os.path.join('etl', 'data', self.input_filename) if self.input_filename else None
        self.output_path = os.path.join('etl', 'output', self.output_filename) if self.output_filename else None
        self.df = df

    @property
    @abstractmethod
    def pagepath_col(self):
        pass

    def load_data(self):
        if self.df is not None:
            print("Using provided DataFrame.")
            return self.df
        if not self.data_path:
            raise ValueError("No input file or DataFrame provided.")
        self.df = pd.read_csv(self.data_path, header=9)
        return self.df

    def drop_if_not_ends_with_html(self):
        initial_len = len(self.df)
        self.df = self.df[self.df[self.pagepath_col].str.endswith('.html', na=False)]
        final_len = len(self.df)
        print(f"Dropped {initial_len - final_len} rows that did not end with '.html'.")
        if final_len == 0:
            print("Warning: No rows left after dropping non-HTML pagepaths.")
        return self.df

    def remove_homepage(self):
        initial_len = len(self.df)
        self.df = self.df[self.df[self.pagepath_col] != '/']
        final_len = len(self.df)
        print(f"Dropped {initial_len - final_len} rows that were equal to '/'.")
        return self.df

    def apply_transformations(self):
        print("Applying transformations...")
        self.drop_if_not_ends_with_html()
        self.remove_homepage()
        print("Transformations applied.")
        print(self.df.head())
        print(f"Columns in DataFrame: {self.df.columns.tolist()}")
        return self.df

    def save_transformed_data(self):
        if not self.output_path:
            raise ValueError("No output path specified.")
        self.df.to_csv(self.output_path, index=False)
        print(f"Transformed data saved to {self.output_path}")

    def run_etl(self):
        self.load_data()
        self.apply_transformations()
        if self.output_path:
            self.save_transformed_data()
        return self.df

class ItalianPageAndScreenETL(BasePageAndScreenETL):
    @property
    def pagepath_col(self):
        return 'Percorso pagina e classe schermata'

class EnglishPageAndScreenETL(BasePageAndScreenETL):
    @property
    def pagepath_col(self):
        return 'pagePath'

class PageAndScreenETLFactory:
    @staticmethod
    def get_etl(language, input_filename=None, output_filename=None, df=None):
        if language == 'it':
            return ItalianPageAndScreenETL(input_filename, output_filename, df)
        elif language == 'en':
            return EnglishPageAndScreenETL(input_filename, output_filename, df)
        else:
            raise ValueError(f"Unsupported language: {language}")

# Example usage:
# etl = PageAndScreenETLFactory.get_etl('it', 'input.csv')
# etl.run_etl()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='ETL for GA4 page and screen data (it/en)')
    parser.add_argument('--lang', choices=['it', 'en'], required=True, help='Language: it or en')
    parser.add_argument('--input', help='Input CSV filename')
    parser.add_argument('--output', help='Output CSV filename (optional)')
    args = parser.parse_args()
    etl = PageAndScreenETLFactory.get_etl(args.lang, args.input, args.output)
    etl.run_etl()
