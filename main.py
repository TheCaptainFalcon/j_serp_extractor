import pandas as pd
import numpy as np
import json
import requests
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import traceback
from datetime import date
from datetime import timedelta
import plotly.express as px
import plotly.graph_objects as go
import datapane as dp

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
query_1 = os.getenv('QUERY_1')
query_2 = os.getenv('QUERY_2')

# python 3+ need to install pymysql and add to mysql
engine = create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}'.format(user, password, host, port, database))

# refactor to function form
def get_jobs_df(url):
    response = requests.get(url=url)
    content = json.loads(response.content)
    df = pd.DataFrame.from_dict(content['jobs_results'])
    df = pd.concat([pd.DataFrame(df), pd.json_normalize(df['detected_extensions'])], axis=1)
    try:
        # sometimes these columns wouldnt exist in certain results, therefore resulting in a keyerror. Implementing a for loop to check if column exists within df.columns, then remove
        columns_to_drop= ['detected_extensions','extensions', 'job_highlights', 'related_links', 'thumbnail', 'job_id', 'work_from_home']
        for column in columns_to_drop:
            if column in df.columns:
                # can use columns to skip specifying axis
                df = df.drop(columns=column)
    except (KeyError, ValueError):
        print('An exception was raised')
        pass
    df['inserted_at'] = date.today()
    # test
    print(df)
    return df

# active json -- 100 api calls/month (4x at 5 wkday = 80 (with 20 as failsafe))
# location based 1
local_1 = f'https://serpapi.com/search?engine=google_jobs&q={query_1}&location=atlanta+ga+united+states&google_domain=google.com&hl=en&gl=us&lrad=49&device=desktop&api_key={api_key}'

# location based 2
local_2 = f'https://serpapi.com/search?engine=google_jobs&q={query_2}&location=atlanta+ga+united+states&google_domain=google.com&hl=en&gl=us&lrad=49&device=desktop&api_key={api_key}'

# remote based 1
remote_1 = f'https://serpapi.com/search?engine=google_jobs&q={query_1}&location=united+states&google_domain=google.com&hl=en&gl=us&ltype=1&lrad=49&device=desktop&api_key={api_key}'

# remote based 2
remote_2 = f'https://serpapi.com/search?engine=google_jobs&q={query_2}&location=united+states&google_domain=google.com&hl=en&gl=us&ltype=1&lrad=49&device=desktop&api_key={api_key}'

# api data
ldf_1 = get_jobs_df(local_1)
ldf_2 = get_jobs_df(local_2)
rdf_1 = get_jobs_df(remote_1)
rdf_2 = get_jobs_df(remote_2)

# change to append after finalising sample source
def load_sql(source):
    source.to_sql('jobs', con=engine, if_exists='append')

load_sql(ldf_1)
load_sql(ldf_2)
load_sql(rdf_1)
load_sql(rdf_2)

# In order to use wildcards, must use %(insert name)s -- dictionary can be setup within the params flag, but its cleaner to separate into a variable
combination_filter = (
    "WITH tech_stack as ( \
        SELECT DISTINCT \
            title, \
            company_name, \
            location, \
            via, \
            description, \
            posted_at, \
            substring_index(posted_at, ' ', 1) as posted_at_int, \
            inserted_at, \
            schedule_type, \
            salary \
        from jobs \
        WHERE description LIKE %(sql)s OR \
        description LIKE %(excel)s OR \
        description LIKE %(tableau)s OR \
        description LIKE %(python)s OR \
        description LIKE %(powerbi)s \
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
            schedule_type, \
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
        schedule_type, \
        salary \
    FROM yoe \
    WHERE description NOT LIKE %(masters_1)s AND \
    description NOT LIKE %(masters_2)s AND \
    description NOT LIKE %(masters_3)s AND \
    description NOT LIKE %(masters_4)s"
)

# __ accounts for if they use apostrophe after the s (plus a space)
combination_dict = {
    'sql' : '%' + 'sql' + '%',
    'excel' : '%' + 'excel' + '%',
    'tableau' : '%' + 'tableau' + '%',
    'python' : '%' + 'python' + '%',
    'powerbi' : '%' + 'power bi' + '%',
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

# mode w is default which is overwrite
# header False is to exclude header when appending new data

def filtered_jobs_to_csv():
    try:
        combination_jobs.to_csv('filtered_jobs.csv', index=False, mode='a', header=False)
    except Exception:
        print('An error occurred trying to export the filtered job results to a csv', '\n')
        traceback.print_exc()
    else:
        print('Filtered job CSV has been updated!')

filtered_jobs_to_csv()

# creates a csv to manipulate data and not disrupt original data
job_report = pd.read_csv('filtered_jobs.csv')
job_report.loc[job_report['location'].str.lower() == 'anywhere', 'remote'] = "Yes"
job_report.loc[job_report['location'].str.lower() != 'anywhere', 'remote'] = "No"
job_report['inserted_at'] = pd.to_datetime(job_report['inserted_at']).dt.normalize()

def date_formatter(target_date):
   return target_date.strftime('%Y-%m-%d')

today = date.today()
format_today = date_formatter(today)
format_yesterday = date_formatter((today - timedelta(days = 1)))
format_two_days_ago = date_formatter((today - timedelta(days = 2)))
format_week_ago = date_formatter((today - timedelta(weeks = 1)))
format_two_weeks_ago = date_formatter((today - timedelta(weeks = 2)))

def date_job_count(target_date):
   return len(job_report[job_report['inserted_at'] == target_date])

today_job_count = date_job_count(format_today)
yesterday_job_count = date_job_count(format_yesterday)
two_days_ago_job_count =  date_job_count(format_two_days_ago)

# total count
total_job_count = len(job_report)

# Create Weekly Job chart for Report
weekly_report = job_report.loc[(job_report['inserted_at'] >= format_week_ago) & (job_report['inserted_at'] <= format_today)]
weekly_fig = px.histogram(
    weekly_report,
    x = 'inserted_at',
    nbins = 20,
    title = 'Weekly Job Report',
    color = 'remote',
    hover_data = {'inserted_at' : '' }
).update_layout(yaxis_title='# of jobs added', title_font_size = 25, xaxis_title = '')

# Compile the Report
app = dp.App(
    dp.Group(
        dp.BigNumber(
            heading = 'Jobs added today',
            value = today_job_count,
            change = yesterday_job_count,
            is_upward_change = True
        ),
    ),
    dp.Plot(weekly_fig),
    dp.DataTable(weekly_report)
)

app.upload(name = "Weekly Job Report")

# revision of salary data by utilizing existing salary field instead of using description
# remove nulls for salary
job_report = job_report.dropna()
sal = job_report

for i, v in sal['salary'].items():
    if 'an hour' in v:
        sal.loc[i, 'salary_rate'] = 'hour'
        raw = v.replace('an hour', '').strip()
        sal.loc[i, 'salary_raw'] = raw
    if 'a year' in v:
        sal.loc[i, 'salary_rate'] = 'year'
        raw = v.replace('a year', '').strip().strip()
        sal.loc[i, 'salary_raw'] = raw

# K removal in annual salary
for i, v in sal['salary_raw'].items():
    if 'K' in v:
        filter = sal['salary_raw'][i].lower().replace('k', '')
        sal.loc[i, 'salary_raw'] = filter
    else:
        continue

value_arr = []

for i, v in sal['salary_raw'].items():
    # for whatever reason the default dash in the data is an uncommon dash.
    values1 = v.replace('–', '-')
    values1 = values1.split('-')
    value_arr.append(values1)

sal['salary_raw_split'] = value_arr

salary_min = []
salary_max = []

for i, v in sal['salary_raw_split'].items():
    if ',' in str(v):
        salary_min.append(v[0])
        salary_max.append(v[1])
    else:
        salary_min.append(v[0])
        salary_max.append(v[0])

sal['salary_min'] = salary_min
sal['salary_max'] = salary_max

# next step creating modified min/max and removing commas
for i, v in sal['salary_min'].items():
    remove_comma = v.replace(',' , '')
    sal.at[i, 'salary_min_mod'] = remove_comma

for i,v in sal['salary_max'].items():
    remove_comma = v.replace(',' , '')
    sal.at[i, 'salary_max_mod'] = remove_comma

# salary new is the calculation stage

salary_new = []

for i, row in sal.iterrows():
    sal_min = row['salary_min_mod']
    sal_max = row['salary_max_mod']
    salary_new.append(int((int(float(sal_min)) + int(float(sal_max))) / 2))

sal['salary_new'] = salary_new

#  salary stnd is the finalized salary column
salary_stnd = []

for i, row in sal.iterrows():
    salary = row['salary'].lower()
    sal_rate = row['salary_rate']
    sal_new = row['salary_new']

    if sal_rate == 'hour':
        sal_calc = sal_new * 2080
        salary_stnd.append(sal_calc)
    if sal_rate == 'year':
        if 'K' in sal['salary'][i]:
            sal_calc = sal_new * 1000
            salary_stnd.append(sal_calc)
        if 'K' not in sal['salary'][i]:
            salary_stnd.append(sal_new)

sal['salary_stnd'] = salary_stnd

# refactor, performance wise this is an improvement over previous code. List comprehension
tech_stack = { 
    'sql' : [], 
    'excel' : [], 
    'python' : [] , 
    'tableau' : [],
    'power bi' : []
}

def tech_arr():
    job_des = job_report['description']
    for value in tech_stack:
        tech_stack[value] = [1 if value in x.lower() else 0 for x in job_des]

tech_arr()

# create the skills columns

job_report = job_report.assign(**tech_stack)

# create a tech stack counter column for scatter plot
tech_count_arr = []

for i, row in job_report.iterrows():
    sql = row['sql']
    excel = row['excel']
    python = row['python']
    tableau = row['tableau']
    powerbi = row['power bi']
    tech_stack_count = (sql + excel +python + tableau + powerbi)
    tech_count_arr.append(tech_stack_count)

job_report['tech_counter'] = tech_count_arr

# export after tech skills
# export as excel for google drive (csv columns get wonky with commas)
# pandas v1.4+ if_sheet_exists: 'overlay' (for append)

with pd.ExcelWriter('filtered_jobs.xlsx', mode='a', if_sheet_exists='overlay') as writer:
    job_report.to_excel(writer, index=False)