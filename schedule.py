import requests, json, pandas as pd, ast, warnings, psycopg2
from pandas import json_normalize
from sqlalchemy import create_engine
import numpy as np
import requests

warnings.simplefilter(action='ignore', category=FutureWarning)

def execute(engine, YOURTOKEN, season):
    result = 0

    r15 = requests.get(f'https://api.sportmonks.com/v3/football/schedules/seasons/{season}?api_token={YOURTOKEN}&include=')
    j = r15.json()
    cabecalho = pd.DataFrame()
    cabecalho = pd.DataFrame.from_dict(j['data'])
    cabecalho = cabecalho.reset_index()

    df_round = pd.json_normalize(cabecalho['rounds'])
    df_rounds = pd.DataFrame()
    df_rounds = pd.json_normalize(df_round[0]).add_prefix(f'rounds_{0}_')
    df_rounds = df_rounds.drop([f'rounds_{0}_fixtures'], axis='columns')
    df_fixtures = pd.DataFrame()
    for i in range(1, len(df_round.columns)):
        df_rounds = df_rounds.join(pd.json_normalize(df_round[i]).add_prefix(f'rounds_{i}_'), lsuffix='_wt',how='left')
        df_fixture = pd.json_normalize(df_rounds[f'rounds_{i}_fixtures'])
        df_rounds = df_rounds.drop([f'rounds_{i}_fixtures'], axis='columns')
        df_fixtures = pd.DataFrame()
        df_fixtures = pd.json_normalize(df_fixture[0]).add_prefix(f'fixture_{0}_')
        df_fixtures = df_fixtures.drop([f'fixture_{0}_participants'], axis='columns')
        df_fixtures = df_fixtures.drop([f'fixture_{0}_scores'], axis='columns')
        for j in range(1, len(df_fixture.columns)):
            df_fixtures = df_fixtures.join(pd.json_normalize(df_fixture[j]).add_prefix(f'fixture_{j}_'), lsuffix='_wt',how='left')
            df_fixtures = df_fixtures.drop([f'fixture_{j}_participants'], axis='columns')
            df_fixtures = df_fixtures.drop([f'fixture_{j}_scores'], axis='columns')

    df_schedules = cabecalho.drop(['rounds'], axis='columns')

    df_schedules = df_schedules.join(df_rounds, lsuffix='_wt',how='left')
    df_schedules = df_schedules.join(df_fixtures, lsuffix='_wt',how='left')
    df_schedules.to_sql('schedules', engine, schema = 'sportMonx', if_exists='append', index=False)

    return result


