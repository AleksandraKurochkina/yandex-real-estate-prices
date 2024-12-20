#part1_airflow/plugins/steps/clean_etl.py

from sqlalchemy import MetaData, Table, Column, Integer, Float, String, inspect, Boolean, Numeric
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

def create_table(**kwargs):
    hook = PostgresHook('destination_db')
    conn = hook.get_sqlalchemy_engine()
    metaData = MetaData()
    flats_clean_table = Table(
        'flats_clean_table',
        metaData,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('building_id', Integer),
        Column('floor', Integer),
        Column('kitchen_area', Float),
        Column('living_area', Float),
        Column('rooms', Integer),
        Column('is_apartment', Boolean),
        Column('studio', Boolean),
        Column('total_area', Float),
        Column('price', Numeric),
        Column('build_year', Integer),
        Column('building_type_int', Integer),
        Column('latitude', Float),
        Column('longitude', Float),
        Column('ceiling_height', Float),
        Column('flats_count', Integer),
        Column('floors_total', Integer),
        Column('has_elevator', Boolean)
    )
    if not inspect(conn).has_table(flats_clean_table.name):
        metaData.create_all(conn)
    
def extract(**kwargs):
    hook = PostgresHook('destination_db')
    conn = hook.get_conn()
    sql = f"""
    select
        a.id, a.building_id, a.floor, a.kitchen_area, a.living_area, a.rooms,
        a.is_apartment, a.studio, a.total_area, a.price, b.build_year, b.building_type_int, 
        b.latitude, b.longitude, b.ceiling_height, b.flats_count, b.floors_total, b.has_elevator
    from flats as a
    left join buildings as b on a.building_id = b.id
    """
    data = pd.read_sql(sql, conn)
    conn.close()
    ti = kwargs['ti']
    ti.xcom_push('extracted_data', data)

def transform(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract', key='extracted_data')
    
    #duplicates
    features = data.drop(columns='id').columns.tolist()
    is_duplicated = data.duplicated(subset=features, keep='first')
    data = data[~is_duplicated].reset_index(drop=True)

    #0 in prices
    data = data[data['price'] != 0]

    #0 to nans
    non_zero_features = ['kitchen_area', 'living_area', 'rooms', 'total_area', 'build_year', 'ceiling_height', 
                     'flats_count', 'floors_total']
    data[non_zero_features] = data[non_zero_features].replace(0, float('nan'))

    #nans
    col_with_nans = data.isnull().sum()
    col_with_nans = col_with_nans[col_with_nans > 0].index
    for col in col_with_nans:
        if data[col].dtype in ['float', 'int']:
            fill_value = data[col].mean()
        if data[col].dtype == 'object':
            fill_value = data[col].mode().iloc[0]
        data[col] = data[col].fillna(fill_value)

    #outliers
    num_cols = data.select_dtypes(['float']).columns
    threshold = 1.5
    potential_outliers = pd.DataFrame()

    for col in num_cols:
        Q1 = data[col].describe().loc['25%']
        Q3 = data[col].describe().loc['75%']
        IQR = Q3 - Q1
        margin = IQR * threshold
        down = Q1 - margin
        up = Q3 + margin
        potential_outliers[col] = ~data[col].between(down, up)
    outliers = potential_outliers.any(axis=1)
    data = data[~outliers]

    ti.xcom_push('transformed_data', data)

def load(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform', key='transformed_data')
    hook = PostgresHook('destination_db')
    hook.insert_rows(
        table = 'flats_clean_table',
        replace=True,
        target_fields=data.columns.tolist(),
        replace_index=['id'],
        rows=data.values.tolist()
    )