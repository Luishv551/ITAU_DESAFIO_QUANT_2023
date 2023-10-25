import pandas as pd

# Load the CSV file into a DataFrame
df = pd.read_csv('merged_output.csv', delimiter=',')  # Adjust the file name and delimiter as needed

# Group by ticker and check for any empty values in each group
ticker_groups = df.groupby('ticker').apply(lambda group: group.notna().all().all())

# Get tickers with no empty values in any row
valid_tickers = ticker_groups[ticker_groups].index

# Filter the DataFrame to keep only rows for valid tickers
filtered_df = df[df['ticker'].isin(valid_tickers)]

# Save the filtered DataFrame to a new CSV file
filtered_df.to_csv('BASE_FINAL_COMPLETA.csv', index=False)

print('Filtered CSV saved to BASE_FINAL_COMPLETA.csv')
