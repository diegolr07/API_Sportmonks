import requests, json, pandas as pd, ast, warnings, psycopg2
from pandas import json_normalize
from sqlalchemy import create_engine
import numpy as np
import requests

warnings.simplefilter(action='ignore', category=FutureWarning)

def execute(engine, YOURTOKEN):
    result = 0
    r = requests.get(f'https://api.sportmonks.com/v3/my/leagues?api_token={YOURTOKEN}&include=')
    j = r.json()
    cabecalho = pd.DataFrame()

    cabecalho = pd.DataFrame.from_dict(j['data'])
    cabecalho = cabecalho.reset_index()

    cabecalho = cabecalho.drop(['sport'], axis='columns')
    df_leagues = cabecalho

    df_leagues.to_sql('leagues', engine, schema = 'sportMonx', if_exists='append', index=False)

    return result


