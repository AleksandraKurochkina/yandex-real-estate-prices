#part2_dvc/scripts/fit.py
import yaml
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
import xgboost as xgb
from sklearn.pipeline import Pipeline
import os
import joblib


def fit_model():
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)
    data = pd.read_csv('part2_dvc/data/initial_data.csv')
    float_cols = data.select_dtypes(include='float').columns.to_list()
    #all cat cols is yes/no cols
    cat_cols = data.select_dtypes(include='object').columns.to_list()
    preprocessor = ColumnTransformer(
        [
            ('num_cols_norm', StandardScaler(), float_cols),
            ('cat_cols_encode', OneHotEncoder(drop=params['one_hot_drop']), cat_cols)
        ]
    )
    model = xgb.XGBRegressor(n_estimators=params['n_estimators'], random_state=params['random_state'])
    pipeline = Pipeline(
        [
            ('preprocessor', preprocessor),
            ('model', model)
        ]
    )
    pipeline.fit(data, data[params['target_col']])

    os.makedirs('part2_dvc/models', exist_ok=True)
    with open ('part2_dvc/models/fitted_model.pkl', 'wb') as fd:
        joblib.dump(pipeline, fd)

if __name__ == '__main__':
    fit_model()