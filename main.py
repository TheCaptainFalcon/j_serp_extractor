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
        DISTINCT SELECT \
            title, \
            company_name, \
            location, \
            via, \
            description, \
            posted_at, \
            substring_index(posted_at, ' ', 1) as posted_at_int, \
            inserted_at \
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
            inserted_at \
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
        inserted_at \
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

# filtered_jobs_to_csv()

# creates a csv to manipulate data and not disrupt original data

job_report = pd.read_csv('filtered_jobs.csv')
job_report.loc[job_report['location'].str.lower() == 'anywhere', 'remote'] = "Yes"
job_report.loc[job_report['location'].str.lower() != 'anywhere', 'remote'] = "No"
job_report['inserted_at'] = pd.to_datetime(job_report['inserted_at']).dt.normalize()

def date_formatter(target_date):
    target_date.strftime('%m/%d/%Y')

today = date.today()
format_today = date_formatter(today)
format_yesterday = date_formatter((today - timedelta(days = 1)))
format_two_days_ago = date_formatter((today - timedelta(days = 2)))
format_week_ago = date_formatter((today - timedelta(weeks = 1)))
format_two_weeks_ago = date_formatter((today - timedelta(weeks = 2)))

def date_job_count(target_date):
    len(job_report[job_report['inserted_at'] == target_date])

today_job_count = date_job_count(format_today)
yesterday_job_count = date_job_count(format_yesterday)
two_days_ago_job_count =  date_job_count(format_two_days_ago)

# needs ranged conditional

def ranged_date_job_count(target_date_before, target_date_after):
    job_count_by_date = len(job_report[job_report['inserted_at']])
    (job_count_by_date >= target_date_before) & (job_count_by_date <= target_date_after)

week_job_count = ranged_date_job_count(format_week_ago, format_today)
two_weeks_ago_job_count = ranged_date_job_count(format_two_weeks_ago, format_week_ago)
# total count
total_job_count = len(job_report)

# old code (prior to refactor)
# yesterday = today - timedelta(days = 1)
# two_days_ago = today - timedelta(days = 2)
# week_ago = today = timedelta(weeks = 1)
# two_weeks_ago = today = timedelta(weeks = 2)

# format_today = today.strftime("%m/%d/%Y")
# format_yesterday = yesterday.strftime('%m/%d/%Y')
# format_two_days_ago = two_days_ago.strftime('%m/%d/%Y')
# format_week_ago = week_ago.strftime('%m/%d/%Y')
# format_two_weeks_ago = two_weeks_ago.strftime('%m/%d/%Y')


# Data integrity/check
today_counter = (yesterday_job_count -  today_job_count)

def curr_counter():
    if today_counter == 0:
        print(today_counter)
    if today_counter < 0:
        print('-', today_counter)
    else:
        print(today_counter, '+')

# curr_counter()

# Create Remote columns
job_loc = job_report[job_report['location'].str.lower()]
remote = job_loc == 'anywhere'
onsite = job_loc != 'anywhere'


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
            heading = 'Weekly Job Count:',
            value = week_job_count,
            change = two_weeks_ago_job_count,
            is_upward_change = True
        ),
        dp.BigNumber(
            heading = 'Jobs added today',
            value = today_job_count,
            change = yesterday_job_count,
            is_upward_change = True
        ),
        columns = 2,
    ),
    dp.Plot(weekly_fig),
    dp.DataTable(weekly_report)
)

app.upload(name = "Weekly Job Report")

# Onward to dashboard code
# Salary column - search, clean, and assign
jb = job_report['description']
salary_arr = []

# -1 means False; could not find specified str
for i, v in enumerate(jb):
    # within the description column, find if theres both a dollar sign and the word salary
    if v.lower().find('$') != -1 and v.lower().find('salary') != -1:
        # if true, see if theres a per year contained inside
        if v.lower().find('per year'):
            ds_1 = v.lower().find('$')
            ds_2 = v.lower().rfind('$')
            # if true, slice a chunk of the data based on both the first and second instances of the dollar signs
            ds_salary_1 = jb[i][ds_1 : ds_1 + 7]
            ds_salary_2 = jb[i][ds_2 : ds_2 + 7]
            # after checking if it has a per year, check if theres a comma contained within the sliced data
            if ds_salary_1.find(',') != -1:
                comma_salary_1 = int(ds_salary_1.replace('$' , '').replace(',' , ''))
                comma_salary_2 = int(ds_salary_2.replace('$' , '').replace(',' , ''))
                avg_comma_salary = int((comma_salary_1 + comma_salary_2) / 2)
                salary_arr.append(avg_comma_salary)
            # if it doesnt have a comma, then look to see if theres a k in there
            if ds_salary_1.find('k') != -1:
                k_salary_1 = ds_salary_1.replace('$' , '').replace('k' , '')
                k_salary_2 = ds_salary_2.replace('$' , '').replace('k' , '')
            # after finding the k, it needs to further slice the data to be usable
                k_salary_new_1 = int(k_salary_1[0:2])
                k_salary_new_2 = int(k_salary_2[0:2])
                avg_k_salary = int(((k_salary_new_1 + k_salary_new_2) / 2) * 1000)
                salary_arr.append(avg_k_salary)
        # after finding a dollar sign and salary, but not a per year, this checcks if it has a per hour
        if v.lower().find('per hour') != -1:
            hr_salary_1 = int(jb[i][ds_1:ds_1+3].replace('$' , '').strip())
            hr_salary_2 = int(jb[i][ds_2:ds_2+3].replace('$' , '').strip())
            avg_hr_salary = int(((hr_salary_1 + hr_salary_2) / 2) * 2080)
            salary_arr.append(avg_hr_salary)
    # if it doesnt contain a dollar sign or salary from the start, then skip this and call it null
    else:
        salary_arr.append('null')

# create the salary column
job_report['new_salary'] = salary_arr


# Skillset columns - Check to see which rows contain which of the skills
sql = []
excel = []
python = []
tableau = []
tech_stack = ['sql', 'excel', 'python', 'tableau']
tech_stack_var = [sql, excel, python, tableau]

def tech_arr():
    job_des = job_report['description']
    # loop through the all the rows in column 'description'
    for i, v in enumerate(job_des):
        # loop through all the items in the tech_stack array
        for index, value in enumerate(tech_stack):
            # within each description index look to see if theres any of the tech stacks contained, if it does then apply a 1 to the tech specific array
            if job_des[i].lower().find(value) != -1:
                tech_stack_var[index].append(1)
            else:
                tech_stack_var[index].append(0)

tech_arr()

# create the skills columns
job_report['sql'] = sql
job_report['excel'] = excel
job_report['python'] = python
job_report['tableau'] = tableau

# create the visuals
# Data Analyst Jobs by Date

total_jobs = px.histogram(
    job_report, 
    x='inserted_at', 
    nbins=20, 
    title='Data Analyst Jobs by Date',
    color='remote',
    hover_data={'inserted_at' : ''}
).update_layout(
    yaxis_title='Count', 
    title_font_size=25, 
    xaxis_title='', 
    font=dict(size=14)
)

# Data Analytics Interest by Date (Google Search Trends)
pytrends = TrendReq()

kw_list=['data analytics', 'data science']
pytrends.build_payload(
    kw_list, 
    cat=0, 
    timeframe='today 12-m'
)

trend_data = pytrends.interest_over_time()
trend_data = trend_data.reset_index()

trend_chart = px.line(
    trend_data, 
    x='date', 
    y=kw_list, 
    title="Data Analytics Interest by Date"
).update_layout(
    xaxis_title='',
    title_font_size=25, 
    yaxis_title='Interest', 
    font=dict(size=14)
)

# Jobs by Skills
skill_sum = job_report[['sql','excel','python', 'tableau']].sum()

skills = px.bar(
    skill_sum, 
    title='Jobs by Skills',
    y=skill_sum[:]
).update_layout(
    yaxis_title='Count', 
    title_font_size=25, 
    xaxis_title='', 
    xaxis={'categoryorder': 'total descending'}, 
    font=dict(size=14)
)

# Data Analyst Jobs by Skills
avg_salary = px.histogram(
    job_report, 
    title='Data Analyst Jobs by Salary',
    x='new_salary'
).update_layout(
    yaxis_title='Count', 
    title_font_size=25, 
    xaxis_title='',
    font=dict(size=14)
)

# Compiled Dashboard
dash = dp.App(
        dp.Group(
            dp.BigNumber(
                heading='Total Job Count:',
                value=total_job_count
            ),
            dp.BigNumber(
                heading="Jobs added today",
                value=today_job_count
            ),         
            columns=2,
        ),
        dp.Group(
            dp.Plot(total_jobs), 
            dp.Plot(trend_chart),
            columns=2,
        ),
        dp.Group(
            dp.Plot(skills),
            dp.Plot(avg_salary),
            columns=2,  
        ),
        dp.DataTable(job_report)
)

dash.upload(name="Data Analytics Job Dashboard")