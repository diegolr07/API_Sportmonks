import requests, json, pandas as pd, ast, warnings, psycopg2
from pandas import json_normalize
from sqlalchemy import create_engine
import numpy as np
import season, leagues, teams

#league 71/ season 2022/ team 33
warnings.simplefilter(action='ignore', category=FutureWarning)
# Configuração do TOKEN de Autenticacao da API
YOURTOKEN = 'vTI851oUX6khcRTuP03VhhxE9C1QrRwcppzsKOJD7F51C7OMI2glxRo2IyrZ'
dbschema='apiFootball,sportMonx,public' # Searches left-to-right
engine = create_engine(
    'postgresql+psycopg2://diego_rocha:admin123@138.0.218.217:5432/esporte',
    connect_args={'options': '-c search_path={}'.format(dbschema)})

teams.execute(engine, YOURTOKEN, 16996)

