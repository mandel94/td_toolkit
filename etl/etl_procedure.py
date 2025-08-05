import pandas as pd
import os
from td_data_toolkit.transformations import apply_transformations

def etl_procedure(input_filename, output_filename):
    # Define paths
    data_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', input_filename)
    output_path = os.path.join(os.path.dirname(__file__), '..', '..', 'output', output_filename)

    # Read data
    df = pd.read_csv(data_path)

    # Apply transformations (currently does nothing)
    df_transformed = apply_transformations(df)

    # Save output
    df_transformed.to_csv(output_path, index=False)
    return df_transformed
