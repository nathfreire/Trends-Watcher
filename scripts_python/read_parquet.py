#pip install pandas pyarrow

import pandas as pd

# Cambia la ruta por la de tu archivo
df = pd.read_parquet('output/resultado_parquet/part-00000-be435e1d-a977-436f-934f-b2ce9f00bc0a-c000.snappy.parquet')
print(df.head())