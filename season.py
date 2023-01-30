import requests, json, pandas as pd, ast, warnings, psycopg2
from pandas import json_normalize
from sqlalchemy import create_engine
import numpy as np
import requests

warnings.simplefilter(action='ignore', category=FutureWarning)

def execute(engine, YOURTOKEN):
    result = 0
    r = requests.get(f'https://api.sportmonks.com/v3/football/seasons?api_token={YOURTOKEN}')
    j = r.json()
    i = 1
    while j['pagination']['has_more'] != False:
        r15 = requests.get(f'https://api.sportmonks.com/v3/football/seasons?api_token={YOURTOKEN}&page={i}')
        i = i + 1
        j = r15.json()
        cabecalho = pd.DataFrame()

        cabecalho = pd.DataFrame.from_dict(j['data'])
        cabecalho = cabecalho.reset_index()
        df_seasons = cabecalho

        df_seasons.to_sql('seasons', engine, schema = 'sportMonx', if_exists='append', index=False)

    return result


