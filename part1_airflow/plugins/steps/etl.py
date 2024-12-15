# part1_airflow/plugins/steps/etl.py

def create_table(**kwargs):
    from sqlalchemy import MetaData, Table, Column, Integer, String, inspect, Float
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook('destination_db')
    conn = hook.get_sqlalchemy_engine()
    metadata = MetaData()
    flats_table = Table(
        'flats_table',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('building_id', Integer),
        Column('floor', Integer),
        Column('kitchen_area', Float),
        Column('living_area', Float),
        Column('rooms', Integer),
        Column('is_apartment', String),
        Column('studio', String),
        Column('total_area', Float),
        Column('price', Float),
        Column('build_year', Integer),
        Column('building_type_int', Integer),
        Column('latitude', Float),
        Column('longitude', Float),
        Column('ceiling_height', Float),
        Column('flats_count', Integer),
        Column('floors_total', Integer),
        Column('has_elevator', String)
    )
    if not inspect(conn).has_table(flats_table.name):
        metadata.create_all(conn) 

def extract(**kwargs):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    import pandas as pd

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

def load(**kwargs):
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract', key='extracted_data')
    hook = PostgresHook('destination_db')
    hook.insert_rows(
        table="flats_table",
        replace=True,
        target_fields=data.columns.tolist(),
        replace_index=['id'],
        rows=data.values.tolist()
    )
    