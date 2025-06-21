import kagglehub
import pandas as pd
from sqlalchemy import create_engine
import os
os.environ["PGCLIENTENCODING"] = "UTF-8"

def download_read_data(data_path, enc):
    path = kagglehub.dataset_download(data_path) 
    #print("Path to dataset files:", path)
    df = pd.read_csv(f"{path}/Superstore.csv", encoding = enc) #'latin1'
    return df


def clean_text(x):
    if isinstance(x, str):
        return x.encode('ascii', errors='ignore').decode('ascii')
    return x


def load_data(df, login, password, table_name, database_name):
    engine = create_engine(f'')

    try:
        df.to_sql(table_name, 
                engine, 
                if_exists='replace', 
                index=False,
                method='multi')  # Ускоряет загрузку
    
        print("Данные успешно загружены в PostgreSQL!")
    
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        engine.dispose()





data_path = "vivek468/superstore-dataset-final"
enc = "latin1"
login = ""
password = ""
table_name = "superstore"
database_name = "sales_analysis"
df = download_read_data(data_path, enc)
df = df.map(clean_text)
load_data(df, login, password, table_name, database_name)
