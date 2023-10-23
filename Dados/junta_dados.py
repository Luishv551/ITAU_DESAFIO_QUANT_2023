import pandas as pd

# Load the CSV files into DataFrames
file1 = pd.read_csv('EV_EBIT_FILTRO.csv')
file2 = pd.read_csv('historico_acoes_FILTRO.csv')
file3 = pd.read_csv('ROIC_FILTRO.csv')
file4 = pd.read_csv('ValorDeMercado_FILTRO.csv')
file5 = pd.read_csv('EV_FILTRO.csv')

# Merge DataFrames based on 'year', 'month', and 'ticker' columns, using a left join
merged_df = pd.merge(file1, file2, on=['year', 'month', 'ticker'], how='left', suffixes=('_file1', '_file2'))
merged_df = pd.merge(merged_df, file3, on=['year', 'month', 'ticker'], how='left', suffixes=('_file12', '_file3'))
merged_df = pd.merge(merged_df, file4, on=['year', 'month', 'ticker'], how='left', suffixes=('_file123', '_file4'))
merged_df = pd.merge(merged_df, file5, on=['year', 'month', 'ticker'], how='left', suffixes=('_file1234', '_file5'))

# Save the merged DataFrame to a new CSV file
merged_df.to_csv('merged_output.csv', index=False)

print('Merged CSV saved to merged_output.csv')
