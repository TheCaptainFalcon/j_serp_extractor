import pandas as pd
import numpy as np
import json
import requests
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# pathing to direct the env vars
current_path = os.getcwd()
# print(current_path)

load_dotenv(os.path.join(current_path, 'credentials.env'))

# MySQL connect and SerpAPI key
user = os.getenv('USER')
password = os.getenv('PASSWORD')
host = os.getenv('HOST')
port = os.getenv('PORT')
database = os.getenv('DATABASE')
api_key = os.getenv('SERPAPI_KEY')


# python 3+ need to install pymysql and add to mysql
engine = create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}'.format(user, password, host, port, database))

# sample data from source
json_source = 'https://serpapi.com/searches/c2747cffe0cd36d6/6396359cf26ac6a892cab1ca.json'
j = requests.get(url=json_source)
content = json.loads(j.content)

# normalizing the detected_extensions into a readable/usable column
df = pd.DataFrame.from_dict(content['jobs_results'])
df = pd.concat([pd.DataFrame(df), pd.json_normalize(df['detected_extensions'])], axis=1).drop(columns='detected_extensions')
# need to remove as it repeats detected extensions but is a list -- cannot use normalize (and is pointless)
df = df.drop(columns='extensions', axis=1)

tech_stack = [
    'sql', 'excel', 'tableau', 'python'
]

yoe =  [
    '0', '1', '2', '3'
]

# print(df.head(5))

# create table in mysql db -- only needed for initial setup
# engine.execute('create database data_jobs')

# change to append after finalising sample source
# jobs_df = df.to_sql('jobs', con=engine, if_exists='append')

# checking if returns data correctly
# print(engine.execute('SELECT title FROM jobs').fetchall())

# unbelievable... song and dance needed just to pass the % wildcard. Need to do this multiple times tho...
tech_stack_filter = (
    "SELECT title, company_name FROM jobs WHERE description LIKE %(sql)s OR description LIKE %(excel)s OR description LIKE %(tableau)s OR description LIKE %(python)s"
    )

# sql = {'%' + 'sql' + '%'}
# excel = {'%' + 'excel' + '%'}

tech_jobs = pd.read_sql(
    tech_stack_filter, 
    con=engine, 
    params={
        'sql': '%' + 'sql' + '%',
        'excel' : '%' + 'excel' + '%',
        'tableau' : '%' + 'tableau' + '%',
        'python' : '%' + 'python' + '%'
    })

print(tech_jobs)


