import pandas as pd
import os

# Directory where the Parquet files are stored
parquet_directory = 'C:\\Users\\luish\\OneDrive\\Área de Trabalho\\ITAU QUANT\\Dados'

# Get a list of all Parquet files in the directory
parquet_files = [f for f in os.listdir(parquet_directory) if f.endswith('.parquet')]

# Loop through each Parquet file and save as CSV
for parquet_file in parquet_files:
    # Read the Parquet file into a DataFrame
    df = pd.read_parquet(os.path.join(parquet_directory, parquet_file))

    # Generate the corresponding CSV file name
    csv_file = os.path.splitext(parquet_file)[0] + '.csv'

    # Save the DataFrame to a CSV file in the same directory
    df.to_csv(os.path.join(parquet_directory, csv_file), index=False)
