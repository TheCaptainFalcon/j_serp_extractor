import pandas as pd
import numpy as np
import json
import requests
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import traceback
from datetime import date
import streamlit as st

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

# to distinguish data point for when script inserted this record into db
df['inserted_at'] = date.today()

# create table in mysql db -- only needed for initial setup
# engine.execute('create database data_jobs')

# change to append after finalising sample source
# df.to_sql('jobs', con=engine, if_exists='replace')

# In order to use wildcards, must use %(insert name)s -- dictionary can be setup within the params flag, but its cleaner to separate into a variable
combination_filter = (
    "WITH tech_stack as ( \
        SELECT \
            title, \
            company_name, \
            location, \
            via, \
            description, \
            posted_at, \
            substring_index(posted_at, ' ', 1) as posted_at_int, \
            inserted_at, \
            salary \
        from jobs \
        WHERE description LIKE %(sql)s OR \
        description LIKE %(excel)s OR \
        description LIKE %(tableau)s OR \
        description LIKE %(python)s \
    ), \
    yoe as ( \
        SELECT \
            title, \
            company_name, \
            location, \
            via, \
            description, \
            CASE \
                WHEN posted_at LIKE %(hour)s THEN ROUND(posted_at_int / 24, 2) \
                ELSE posted_at_int \
            END AS posted_by_day, \
            inserted_at, \
            salary \
        FROM tech_stack \
        WHERE description LIKE %(yoe_0)s OR \
        description LIKE %(yoe_1)s OR \
        description LIKE %(yoe_2)s OR \
        description LIKE %(yoe_3)s \
    ) \
    SELECT \
        title, \
        company_name, \
        location, \
        via, \
        description, \
        posted_by_day, \
        inserted_at, \
        salary \
    FROM yoe \
    WHERE description NOT LIKE %(masters_1)s AND \
    description NOT LIKE %(masters_2)s AND \
    description NOT LIKE %(masters_3)s AND \
    description NOT LIKE %(masters_4)s"
)

# testing
# combination_filter = (
#     'select * from jobs'
# )

# may need to add another case statement in the last query above for 1|0 determining whether a job is remote (anywhere) or hybrid/onsite

# __ accounts for if they use apostrophe after the s (plus a space)
combination_dict = {
    'sql' : '%' + 'sql' + '%',
    'excel' : '%' + 'excel' + '%',
    'tableau' : '%' + 'tableau' + '%',
    'python' : '%' + 'python' + '%',
    # actually, removing the 2nd % messes up the chain as it returns nothing
    'yoe_0' : '%' + '0' + '%',
    'yoe_1' : '%' + '1' + '%',
    'yoe_2' : '%' + '2' + '%',
    'yoe_3' : '%' + '3' + '%',
    'masters_1' : '%' + "master's" + '%',
    'masters_2' : '%' + "master's__degree" + '%',
    'masters_3' : '%' + "masters_degree" + '%',
    'masters_4' : '%' + 'masters_in' + '%',
    'hour' : '%' + 'hour' + '%'
}

combination_jobs = pd.read_sql(
    combination_filter,
    con=engine,
    params=combination_dict
)

# print(combination_jobs)

def filtered_jobs_to_csv():
    try:
        combination_jobs.to_csv('filtered_jobs.csv', index=False)
    except Exception:
        print('An error occurred trying to export the filtered job results to a csv', '\n')
        traceback.print_exc()
    else:
        print('Filtered job CSV has been updated!')

filtered_jobs_to_csv()

# st.write("""
# # my first app
# Hello asdlkj
# """)

# st.line_chart(df)