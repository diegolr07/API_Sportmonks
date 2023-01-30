import requests, json, pandas as pd, ast, warnings, psycopg2
from pandas import json_normalize
from sqlalchemy import create_engine
import numpy as np
import requests
import rounds, schedule, standings, stages, topscores, venues, squads

warnings.simplefilter(action='ignore', category=FutureWarning)

def execute(engine, YOURTOKEN, season):
    result = 0

    r = requests.get(f'https://api.sportmonks.com/v3/football/teams/seasons/{season}?api_token={YOURTOKEN}')
    j = r.json()
    cabecalho = pd.DataFrame()
    cabecalho = pd.DataFrame.from_dict(j['data'])
    cabecalho = cabecalho.reset_index()
    df_teams = cabecalho
    df_teams.to_sql('teams', engine, schema = 'sportMonx', if_exists='append', index=False)
    schedule.execute(engine, YOURTOKEN, season)
    standings.execute(engine, YOURTOKEN, season)
    stages.execute(engine, YOURTOKEN, season)
    topscores.execute(engine, YOURTOKEN, season)
    venues.execute(engine, YOURTOKEN, season)
    rounds.execute(engine, YOURTOKEN, season)

    for i in range(len(df_teams['id'])):
        squads.execute(engine, YOURTOKEN, season, df_teams['id'][i])

    return result


