Цель проекта — создать базовое решение для предсказания стоимости квартир Яндекс Недвижимости.

DAGs 1: part1_airflow/dags/etl.py
Функции 1: part1_airflow/plugins/steps/etl.py
DAGs 2: part1_airflow/dags/clean_etl.py
Функции 2: part1_airflow/plugins/steps/clean_etl.py

DVC-пайплайн:
  Загрузка данных: part2_dvc/scripts/data.py
  Обучение: part2_dvc/scripts/fit.py
  Оценка модели: part2_dvc/scripts/evaluate.py
dvc.yaml, params.yaml, dvc.lock: yandex-real-estate-prices/
