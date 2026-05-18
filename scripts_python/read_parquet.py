#pip install pandas pyarrow

import pandas as pd

# Cambia la ruta por la de tu archivo
df = pd.read_parquet('output/resultado_parquet/part-00000-b550fd2f-f410-4204-9456-d0ef0ad68721-c000.snappy.parquet')
print(df.head())