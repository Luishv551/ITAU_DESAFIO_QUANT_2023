import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from tqdm import tqdm

# List of file names
file_names = ['EV', 'EV_EBIT', 'historico_acoes', 'ROIC', 'ValorDeMercado']

# Loop through the file names
for file_name in file_names:
    # Read the Parquet file into a DataFrame
    input_file = f'{file_name}.parquet'
    table = pq.read_table(input_file)
    df = table.to_pandas()

    # Convert the 'data' column to datetime
    df['data'] = pd.to_datetime(df['data'])

    # Extract the year and month from the 'data' column
    df['year'] = df['data'].dt.year
    df['month'] = df['data'].dt.month

    # Function to filter for the last day of the month
    def filter_last_day_of_month(group):
        return group[group['data'] == group['data'].max()]

    # Group the data for progress tracking
    grouped = df.groupby(['ticker', 'year', 'month'])

    # Initialize the progress bar
    with tqdm(total=len(grouped), unit="group") as pbar:
        filtered_data = []
        for name, group in grouped:
            filtered_group = filter_last_day_of_month(group)
            filtered_data.append(filtered_group)
            pbar.update(1)  # Update the progress bar

    # Combine the filtered data
    filtered_df = pd.concat(filtered_data, ignore_index=True)

    # Drop duplicates for the same last day of the month
    filtered_df.drop_duplicates(subset=['ticker', 'data'], keep='first', inplace=True)

    # Save the filtered DataFrame to a new Parquet file
    output_file = f'{file_name}_FILTRO.parquet'
    output_table = pa.Table.from_pandas(filtered_df)
    pq.write_table(output_table, output_file)
