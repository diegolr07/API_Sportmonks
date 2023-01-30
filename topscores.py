import requests, json, pandas as pd, ast, warnings, psycopg2
from pandas import json_normalize
from sqlalchemy import create_engine
import numpy as np
import requests

warnings.simplefilter(action='ignore', category=FutureWarning)

def execute(engine, YOURTOKEN, season):
    result = 0

    r15 = requests.get(f'https://api.sportmonks.com/v3/football/topscorers/seasons/{season}?api_token={YOURTOKEN}&include=player')
    j = r15.json()
    i = 1
    while j['pagination']['has_more'] != False:
        r15 = requests.get(f'https://api.sportmonks.com/v3/football/topscorers/seasons/{season}?api_token={YOURTOKEN}&page={i}&include=player')
        j = r15.json()
        i = i + 1
        cabecalho = pd.DataFrame()
        cabecalho = pd.DataFrame.from_dict(j['data'])
        cabecalho = cabecalho.reset_index()
        df_topscorers = cabecalho
        df_player =  pd.json_normalize(df_topscorers['player'])

        df_topscorers = df_topscorers.drop(['player'], axis='columns')
        df_topscorers = df_topscorers.join(df_player, lsuffix='_wt', how='left')
        df_topscorers.to_sql('topscorers', engine, schema='sportMonx', if_exists='append', index=False)

    return result


