#part2_dvc/scripts/data.py
import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine
import os
import pandas as pd

def create_connection():
    load_dotenv()
    host = os.environ.get('DB_DESTINATION_HOST')
    port = os.environ.get('DB_DESTINATION_PORT')
    name = os.environ.get('DB_DESTINATION_NAME')
    user = os.environ.get('DB_DESTINATION_USER')
    password = os.environ.get('DB_DESTINATION_PASSWORD')
    conn = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{name}', connect_args={'sslmode':'require'})
    return conn

def get_data():
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)
    conn = create_connection()
    data = pd.read_sql('Select * from flats_clean_table', conn, index_col=params['index_col'])
    conn.dispose()
    os.makedirs('part2_dvc/data', exist_ok=True)
    data.to_csv('part2_dvc/data/initial_data.csv', index=None)

if __name__ == '__main__':
    get_data()
    