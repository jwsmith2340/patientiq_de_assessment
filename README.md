# PatientIQ Data Engineer Assessment

## Overview
This repo contains the PatientIQ Data Engineer assessment, including all requested deliverables. 

## Task 1
Expected Deliverables:
1. A script that starts with reading the CSV file and concludes by outputting four CSV files representing the 4 tables. **[CSV Creation DAG](https://github.com/jwsmith2340/patientiq_de_assessment/blob/master/airflow/dags/movies_csv_creation.py)**
2. A short README. Here we are. 

## Task 2
Expected Deliverables:
1. A script to load files from #1 (above) into a database. **[DB Upload DAG](https://github.com/jwsmith2340/patientiq_de_assessment/blob/master/airflow/dags/insert_csv_to_db.py)**

## Task 3
Expected Deliverables:
1. One SQL statement per question above, for a total of 5 statements. [SQL Queries](https://github.com/jwsmith2340/patientiq_de_assessment/blob/master/airflow/dags/sql_files/movie_queries.sql)

2. Answers to each of the above five questions:

Which movie(s) had the 3rd highest revenue?
- Titanic

Which movies did not recoup their budget? (Where revenue did not exceed budget).  For brevity, list the first 3 (as ordered by imdb_id) as the response to this question.
- Foolish Wives, The Merry Widow, and Metropolis

What's the average revenue per genre?  List them in order from highest to lowest average revenue.  For brevity, list the first three as the response to this question.
- Adventure: 182900545.74
- Fantasy: 173280838.27
- Animation: 172119567.78

How many movies are in more than one language?
- 9073

You want to understand if there is a seasonal component to movie releases.  For each month, which genre had the highest proportion of releases?  There should be 1 answer for each calendar month.  If there's a tie, list all the genres.
- Drama was the top category all 12 months
