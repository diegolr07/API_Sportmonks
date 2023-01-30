import requests, json, pandas as pd, ast, warnings, psycopg2
from pandas import json_normalize
from sqlalchemy import create_engine
import numpy as np
import requests

warnings.simplefilter(action='ignore', category=FutureWarning)

def execute(engine, YOURTOKEN, season, team):
    result = 0

    r15 = requests.get(f'https://api.sportmonks.com/v3/football/squads/seasons/{season}/teams/{team}?api_token={YOURTOKEN}&include=player;')
    j = r15.json()
    cabecalho = pd.DataFrame()
    cabecalho = pd.DataFrame.from_dict(j['data'])
    cabecalho = cabecalho.reset_index()
    df_squads = cabecalho
    df_player = pd.json_normalize(cabecalho['player'])
    df_squads = df_squads.drop(['player'], axis='columns')
    df_squads = df_squads.join(df_player, lsuffix='_wt',how='left')
    df_squads.to_sql('squads', engine, schema = 'sportMonx', if_exists='append', index=False)

    return result


