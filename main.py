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
from pytrends.request import TrendReq
from time import sleep

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
        df = df.drop(columns=['detected_extensions','extensions', 'job_highlights', 'related_links', 'thumbnail', 'job_id', 'work_from_home'])
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

# union type conversion
# merge can only merge 2 dataframes at a time
# merged_df_local = pd.merge(
#     ldf_1, 
#     ldf_2, 
#     how='outer'
# )

# merged_df_remote = pd.mnrge(
#     rdf_1,
#     rdf_2,
#     how='outer'
# )

# merged_df = pd.merge(
#     merged_df_local,
#     merged_df_remote,
#     how='outer'
# )

# create the inserted_at after the merge (in case timestamp creates two separate instances)
# merged_df['inserted_at'] = date.today()

# create table in mysql db -- only needed for initial setup
# engine.execute('create database data_jobs')

# change to append after finalising sample source
def load_sql(source):
    source.to_sql('jobs', con=engine, if_exists='append')

# ldf_1.to_sql('jobs', con=engine, if_exists='append')

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

# print(combination_jobs)
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

# needs ranged conditional -- not needed
# def ranged_date_job_count(target_date_before, target_date_after):
#     job_count_by_date = len(job_report[job_report['inserted_at']])
#     return (job_count_by_date >= target_date_before) & (job_count_by_date <= target_date_after)

# this and i presume most of the other date based calcs return errors until enough time (and data) has been passed/created
# week_job_count = ranged_date_job_count(format_week_ago, format_today)
# two_weeks_ago_job_count = ranged_date_job_count(format_two_weeks_ago, format_week_ago)

# total count
total_job_count = len(job_report)

# Data integrity/check
# today_counter = (yesterday_job_count -  today_job_count)


# mostly for testing
# def curr_counter():
#     if today_counter == 0:
#         print(today_counter)
#     if today_counter < 0:
#         print('-', today_counter)
#     else:
#         print(today_counter, '+')

# curr_counter()

# Create Remote columns
# def remote_column_creator():
#     try:
#         job_loc = job_report[job_report['location'].str.lower()]
#         remote = job_loc == 'anywhere'
#         onsite = job_loc != 'anywhere'
#     except:
#         print('location not found, retrying in 3s')
#         sleep(3)
#         remote_column_creator()





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
        # dp.BigNumber(
        #     heading = 'Weekly Job Count:',
        #     value = week_job_count,
        #     change = two_weeks_ago_job_count,
        #     is_upward_change = True
        # ),
        dp.BigNumber(
            heading = 'Jobs added today',
            value = today_job_count,
            change = yesterday_job_count,
            is_upward_change = True
        ),
        # columns = 2,
    ),
    dp.Plot(weekly_fig),
    dp.DataTable(weekly_report)
)

app.upload(name = "Weekly Job Report")



# Onward to dashboard code
# Salary column - search, clean, and assign

# testing here
# job_report = pd.read_excel('filtered_jobs.xlsx')


# jb = job_report['description']
# salary_arr = []

# # -1 means False; could not find specified str
# for i, v in enumerate(jb):
#     # within the description column, find if theres both a dollar sign and the word salary
#     if '$' in v.lower() and 'salary' in v.lower():
#         # if true, see if theres a per year contained inside
#         if 'per year' in v.lower():
#             ds_1 = v.lower().find('$')
#             ds_2 = v.lower().rfind('$')
#             # if true, slice a chunk of the data based on both the first and second instances of the dollar signs
#             ds_salary_1 = jb[i][ds_1 : ds_1 + 7]
#             ds_salary_2 = jb[i][ds_2 : ds_2 + 7]
#             # after checking if it has a per year, check if theres a comma contained within the sliced data
#             if ',' in ds_salary_1:
#                 comma_salary_1 = int(ds_salary_1.replace('$' , '').replace(',' , ''))
#                 comma_salary_2 = int(ds_salary_2.replace('$' , '').replace(',' , ''))
#                 avg_comma_salary = int((comma_salary_1 + comma_salary_2) / 2)
#                 salary_arr.append(avg_comma_salary)
#             # if it doesnt have a comma, then look to see if theres a k in there
#             if 'k' in ds_salary_1:
#                 k_salary_1 = ds_salary_1.replace('$' , '').replace('k' , '')
#                 k_salary_2 = ds_salary_2.replace('$' , '').replace('k' , '')
#             # after finding the k, it needs to further slice the data to be usable
#                 k_salary_new_1 = int(k_salary_1[0:2])
#                 k_salary_new_2 = int(k_salary_2[0:2])
#                 avg_k_salary = int(((k_salary_new_1 + k_salary_new_2) / 2) * 1000)
#                 salary_arr.append(avg_k_salary)
#         # after finding a dollar sign and salary, but not a per year, this checcks if it has a per hour
#         if 'per hour' in v.lower():
#             ds_1 = v.lower().find('$')
#             ds_2 = v.lower().rfind('$')
#             hr_salary_1 = int(jb[i][ds_1:ds_1+3].replace('$' , '').strip())
#             hr_salary_2 = int(jb[i][ds_2:ds_2+3].replace('$' , '').strip())
#             avg_hr_salary = int(((hr_salary_1 + hr_salary_2) / 2) * 2080)
#             salary_arr.append(avg_hr_salary)
#     # if it doesnt contain a dollar sign or salary from the start, then skip this and call it null
#     else:
#         salary_arr.append('null')

# # create the salary column
# try:
#     job_report['new_salary'] = salary_arr
# except:
#     print('error', salary_arr)


# revision of salary data by utilizing existing salary field isntead of using description
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

# remove the work building up to the final salary column
# sal = sal.drop(columns=[
#     'salary', 
#     'salary_rate', 
#     'salary_raw', 
#     'salary_raw_split', 
#     'salary_min' ,
#     'salary_max', 
#     'salary_min_mod', 
#     'salary_max_mod', 
#     'salary_new',
# ])


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

# create the visuals
# Data Analyst Jobs by Date

# total_jobs = px.histogram(
#     job_report, 
#     x='inserted_at', 
#     nbins=20, 
#     title='Data Analyst Jobs by Date',
#     color='remote',
#     hover_data={'inserted_at' : ''}
# ).update_layout(
#     yaxis_title='Count', 
#     title_font_size=25, 
#     xaxis_title='', 
#     font=dict(size=14)
# )

# Data Analytics Interest by Date (Google Search Trends)
# pytrends = TrendReq()

# kw_list=['data analytics', 'data science']
# pytrends.build_payload(
#     kw_list, 
#     cat=0, 
#     timeframe='today 12-m'
# )

# current issue is code 429 just for testing gets flagged very easily
# trend_data = pytrends.interest_over_time()
# trend_data = trend_data.reset_index()

# trend_chart = px.line(
#     trend_data, 
#     x='date', 
#     y=kw_list, 
#     title="Data Analytics Interest by Date"
# ).update_layout(
#     xaxis_title='',
#     title_font_size=25, 
#     yaxis_title='Interest', 
#     font=dict(size=14)
# )

# Jobs by Skills
# skill_sum = job_report[['sql','excel','python', 'tableau']].sum()

# skills = px.bar(
#     skill_sum, 
#     title='Jobs by Skills',
#     y=skill_sum[:]
# ).update_layout(
#     yaxis_title='Count', 
#     title_font_size=25, 
#     xaxis_title='', 
#     xaxis={'categoryorder': 'total descending'}, 
#     font=dict(size=14)
# )

# Data Analyst Jobs by Skills
# avg_salary = px.histogram(
#     job_report, 
#     title='Data Analyst Jobs by Salary',
#     x='new_salary'
# ).update_layout(
#     yaxis_title='Count', 
#     title_font_size=25, 
#     xaxis_title='',
#     font=dict(size=14)
# )

# Compiled Dashboard
# dash = dp.App(
#         dp.Group(
#             dp.BigNumber(
#                 heading='Total Job Count:',
#                 value=total_job_count
#             ),
#             dp.BigNumber(
#                 heading="Jobs added today",
#                 value=today_job_count
#             ),         
#             columns=2,
#         ),
#         dp.Group(
#             dp.Plot(total_jobs), 
#             dp.Plot(trend_chart),
#             columns=2,
#         ),
#         dp.Group(
#             dp.Plot(skills),
#             dp.Plot(avg_salary),
#             columns=2,  
#         ),
#         dp.DataTable(job_report)
# )

# dash.upload(name="Data Analytics Job Dashboard")