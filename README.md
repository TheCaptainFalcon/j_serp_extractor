# Job Board Analytics Dashboard

![GitHub last commit](https://img.shields.io/github/last-commit/thecaptainfalcon/j_serp_extractor)

<br>

![Job Board Analytics](/job_analytics.JPG "Job Board Analytics Dashboard")

Note: This image is based on data up to Jan 3, 2023, to see the updated live version, refer to the link below.

[Interactive Tableau Dashboard Link](https://public.tableau.com/app/profile/joseph708/viz/JobBoardAnalyticsDashboard/Dashboard)

<br>

# Table of Contents
1. [What is this project?](#whatis)
2. [What is the painpoint this project aims to solve?](#pain)
3. [What was the solution?](#solution)
4. [The Aftermath - Why the project was expanded](#scope)
5. [Explanation of the Data Pipeline workflow](#pipe)
6. [Who is the target end user for this dashboard?](#enduser)
7. [What actions do you recommend for end users based on the data?](#conclusion)
8. [What can be improved from this project?](#improvement)
9. [Tech Stack](#tech)
10. [SQL query used in the project](#sql)
11. [APIs and Libraries used](#api)
12. [Author notes](#notes)

<br>

<div id='whatis'>

## 1. What is this project?

    This project was originally intended as a short, but effective solution to an existing painpoint faced during the job applying process.

    Eventually, the idea of expanding the project's applicable scope was supported by sheer curiosity and given that 'half the work was already done.' (In theory, anyways)

<div id='pain'>

## 2. What is the painpoint this project aims to solve?

    Well, before we get into that, let's paint the picture and provide some context to truly understand the situation.

    During the job applying process, one of the most common paths to take is to look on one of the many popular job board sites (LinkedIn, Glassdoor, Indeed, etc) to find a particular job of interest.

    You enter the job or key terms that are of interest, maybe apply some search filtering and parameters to narrow down your search and away you go.

    As you are reading the job descriptions, you are finding a recurring pattern where a lot of these jobs do not apply to you in any form, better yet, you have determined that only 30-45% have 'some' relevancy from the total results.

    Now, this doesn't take into effect the 'promoted jobs' that usually populate the top of the search results that 9/10 times are not relevant, but at the end of the day, attempting to apply to jobs in this way is a huge time sink and does not make full use of your time.

    Essentially, before you even start, you are working at a disadvantage!

<div id='solution'>

## 3. What was the solution?

    In the beginning, because this painpoint was an issue that was directly affecting my job applying process, I wanted a low overhead solution using automated reporting to fit my needs.

    The plan was to create a webscraping script or to utilize an API that would achieve the same goals and run the script on a scheduled basis to return the data based on three key categories, which would then be further filtered using a combination of Pandas and SQL.

    These three key categories included: tech stacks, years of experience, and whether a master's degree was required.

    This information would then be fitted into appropriate charts using Plotly and then uploaded into a report using Datapane API.

    This script would run on a daily basis during the weekdays, when most job listing activity occurs and update the report as a result.

    Based on the report, you could look at relevant data such as the job title name, the company name, the tech stack used, when it was posted, etc.

    Better yet, because of the DataTable implemented into the report, you could sort the data columns by asc/desc or use SQL queries to filter based on specific needs.


<div id='scope'>

## 4. The Aftermath - Why the project was expanded

    As I mentioned before, how it was "half way done," and could be expanded to craft a dashboard based on the data, it was technically true.

    The only problem was that because the initial report was built to fit a specfic bias and need, the dashboard would have to widen that net to fit a general job seeking audience as part of the 'target stakeholder.'

    Therefore it's important to note that the report and dashboard are built for different purposes and their data are not dependent on each other at all.

    Anyways, I figured that since the hardest part of obtaining the data was already out of the way, I only needed to clean the data and perform exploratory analysis for potential insights.


### The Initial Approach

As the title suggests, I originally wanted to do a bunch of things, but roadblocks forced me to pivot into a different path.


    1. I wanted to use Google Search Trend data to compare 'Data Analysis' and 'Data Science' as a way to gauge interest levels and compare that with the number of jobs appearing on the script.

    But, when executing the script, there were many times when I would face an Error 429, which basically meant that too many requests were being sent and that traffic was being treated as if a bot was doing that.

    For me, I just felt that I was soft banned, and sometimes would get errors even when I hadn't sent any in a reasonable amount of time.

    Therefore, I felt it was best to exclude as the success rate was less than ideal.


    2. I wanted to filter based on the salaries provided in the job description rather than the salary field. 

    I found that in most instances, there would be information on salary in the form of 'per hour', 'per year', '$xxK - $xxK', etc. within the description portion rather than the salary section.

    But, there were so many variations and edge cases that had to be addressed, which made this process very sensitive to how the data was formatted.

    I ended up dropping the na/null data that didnt fill out the salary section and used that data for the avg salary chart and linear regression of the tech stack vs salary chart.

    While there is the possibility of 'upgrading' to this method in the future, it works fine for now.


    3. Using DataPane and Plotly for the dashboard.

    Maybe I can base this solely on my inexperience of using Plotly, but when I first created the dashboard, I just felt on a visual level, it didn't really 'pop.'

    I mean, sure it got the job done, but the storytelling aspect and refined look wasn't present.

    This was the reason why I pivotted to Tableau, which to be honest had its own host of issues in terms of flexibility, but visually looked far better, in my opinion.

<div id='pipe'>

## 5. Explanation of the Data Pipeline Workflow

    Search Engine Result Pages (SERP) API addresses the limitations in webscraping Google, specifically the job board that we are interested in.

    While the Free Tier has a limit of 100 searches per month, it works out fine with some room for error.

    The script currently executes 4 search queries daily (weekdays) based on 'jr data analyst' and 'sql data' within Atlanta based companies and remote based companies.

    This amounts to 4 * 5 = 20 searches per week, or roughly 80 searches a month with room to spare.

    This initial data is semi-cleaned/transformed using Pandas because of nested dictionaries housing some useful column data.

    The script then uses SQLALCHEMY to house the data within a localhost MYSQL database as part of the 'raw' data.

    Using a nested CTE SQL query, we filter the data based on uniqueness (to address duplicates) and the previously mentioned tech stack, YOE, and master's degree requirement to create a CSV for the Datapane report.

    This script will automatically use that CSV to upload into the report using the Datapane API.

    From there, Pandas is used to further clean and filter the data based on the needs of the dashboard.

    The results are saved into an XLSX file (Excel) instead of CSV as there were formatting issues, but works as intended.

    This script is then ran using a BATCH file that runs the Python script on the determined schedule basis using Windows Scheduler.

    This file is saved within Google Drive that Tableau connects to and (should update automatically, but Tableau Public is so trash) updates the dashboard to reflect the results.

<div id='enduser'>

## 6. Who is the target end user for this dashboard?

    Since we widened the net/scope of the project to work with the dashboard, the end user is generalized to fit the typical job seeking candidate in the Atlanta, GA region interested in a data analyst position.

    While there is not a standardized description of what a data analyst does or does not do, as this is solely dependent on the company, the approach was to go for an entry level position with a tech stack minus the machine learning aspect (which honestly is not data analyst type work).

<div id='conclusion'>

## 7. What actions do you recommend for end users based on the data?

    It depends.

    If the goal is to obtain a data analyst position the fastest way possible given the demand, focus on learning and mastering SQL and a data visualization software in this order of priority (choose one): Power BI > Tableau.

    Because Power BI is a Microsoft product that is built on top of Excel, its a good idea to know Excel to fully capitalize on PBI capabilities.


<div id='improvement'>

## 8. What can be improved from this project?

    1. Being able to apply similar scraping or data retrieval methods to the other job boards.

    This would provide more job data by the numbers, but could also be used to compare the factors amongst the job boards as a whole (ie. where most of the job data source is coming from)


    2. Improving the previously mentioned 'salary' data to utilize the description section instead of the uncommon salary section for serp.


    3. If possible, create a crawler to look at local companies to see if career page or similar exists.

    While it is possible that the job may be available on a job board, there are times when they do not use a job board to narrow down the applicant count to those that are aware and interested in the company and so they are utilize internal recruiters.


<div id='sql'>

## 9. SQL Query used in project:

Note the % sign within the LIKE operator, this was the syntax used as part of SQL Alchemy, but same concept regardless.


    WITH tech_stack as ( 
        SELECT DISTINCT 
            title, 
            company_name, 
            location, 
            via, 
            description, 
            posted_at, 
            substring_index(posted_at, ' ', 1) as posted_at_int, 
            inserted_at, 
            schedule_type, 
            salary 
        from jobs 
        WHERE description LIKE %(sql)s OR 
        description LIKE %(excel)s OR 
        description LIKE %(tableau)s OR 
        description LIKE %(python)s OR 
        description LIKE %(powerbi)s 
    ), 
    yoe as ( 
        SELECT 
            title, 
            company_name, 
            location, 
            via, 
            description, 
            CASE 
                WHEN posted_at LIKE %(hour)s THEN ROUND(posted_at_int / 24, 2) 
                ELSE posted_at_int 
            END AS posted_by_day, 
            inserted_at, 
            schedule_type, 
            salary 
        FROM tech_stack 
        WHERE description LIKE %(yoe_0)s OR 
        description LIKE %(yoe_1)s OR 
        description LIKE %(yoe_2)s OR 
        description LIKE %(yoe_3)s 
    ) 
    SELECT 
        title, 
        company_name, 
        location, 
        via, 
        description, 
        posted_by_day, 
        inserted_at, 
        schedule_type, 
        salary 
    FROM yoe 
    WHERE description NOT LIKE %(masters_1)s AND 
    description NOT LIKE %(masters_2)s AND 
    description NOT LIKE %(masters_3)s AND 
    description NOT LIKE %(masters_4)s
    )

<div id='tech'>

## 9. Tech Stack

    - Python
    - Tableau
    - SQL


<div id='api'>

## 10. APIs and Libraries used

    - SerpAPI
    - Dotenv
    - Requests
    - SQL Alchemy
    - Pandas
    - Numpy
    - Plotly
    - DataPane


<div id='notes'>

## 11. Author notes:

    - Scheduling Basis:
    Using the batch file with Windows Scheduler is so much cleaner to use compared to Cron with the Sleep method.

    The method for Cron would work fine for short scripts with low levels of complexities, because the scheduler is dependent on the function to use.

    While it is true that you could house a master function within the whole script and every existing function would be nested, but that approach is dependent on the script being run on a constant basis.

    This can be faced with recursion issues, depending on how often it is run, which can be alleviated using a form of cloud functions, but if we're focusing on the FREE approach, then there are even more limitations to this.


    - The DASH issue:
    If upgrading the salary method to extract from the description, the dash used in the salary range IS NOT the common dash used.

    This flaw or small detail is indicated in VSC, so if you're wondering why the cleaning process isn't working as expected, remember this and facepalm + smh accordingly.