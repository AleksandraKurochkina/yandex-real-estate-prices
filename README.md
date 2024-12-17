# Проект 1 спринта

Добро пожаловать в репозиторий-шаблон Практикума для проекта 1 спринта. Цель проекта — создать базовое решение для предсказания стоимости квартир Яндекс Недвижимости.

Полное описание проекта хранится в уроке «Проект. Разработка пайплайнов подготовки данных и обучения модели» на учебной платформе.

Здесь укажите имя вашего бакета:  s3-student-mle-20241126-331bab00fa
DAGs 1: part1_airflow/dags/etl.py
Функции 1: part1_airflow/plugins/steps/etl.py
DAGs 2: part1_airflow/dags/clean_etl.py
Функции 2: part1_airflow/plugins/steps/clean_etl.py

DVC-пайплайн:
  Загрузка данных: part2_dvc/scripts/data.py
  Обучение: part2_dvc/scripts/fit.py
  Оценка модели: part2_dvc/scripts/evaluate.py
dvc.yaml, params.yaml, dvc.lock: mle-project-sprint-1-v001/
