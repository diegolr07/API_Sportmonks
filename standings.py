import requests, json, pandas as pd, ast, warnings, psycopg2
from pandas import json_normalize
from sqlalchemy import create_engine
import numpy as np
import requests

warnings.simplefilter(action='ignore', category=FutureWarning)

def execute(engine, YOURTOKEN, season):
    result = 0

    r = requests.get(f'https://api.sportmonks.com/v3/football/standings/seasons/{season}?api_token={YOURTOKEN}')
    j = r.json()
    cabecalho = pd.DataFrame()
    cabecalho = pd.DataFrame.from_dict(j['data'])
    cabecalho = cabecalho.reset_index()
    df_standings = cabecalho
    df_standings.to_sql('standings', engine, schema = 'sportMonx', if_exists='append', index=False)

    return result


