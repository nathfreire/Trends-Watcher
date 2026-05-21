#pip install pandas pyarrow

import pandas as pd

# Cambia la ruta por la de tu archivo
df = pd.read_parquet('output/resultado_parquet/part-00000-25057574-bf4f-481c-8c1a-68c658cbfea7-c000.snappy.parquet')
print(df.head())