import pandas as pd
import os
from datetime import datetime 

raw_dir = 'data/raw'
bronze_dir = 'data/bronze'
csv_path = os.path.join(raw_dir, 'order_details.csv')

df = pd.read_csv(csv_path)

current_date = datetime.now().strftime('%Y-%m-%d')

date_dir = os.path.join(bronze_dir, current_date)
os.makedirs(date_dir, exist_ok=True)

df.to_csv(os.path.join(date_dir, 'order_details.csv'), index=False)

print(f"Arquivo CSV extraído e salvo em {date_dir}")