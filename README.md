# Task 7 Airflow introduction

This is a small application used to transform data from ```csv``` file and import it into MongoDB database using Airflow.

There are two DAGs in ```dags.py```. 

The first DAG uses FileSensor to detect new file in working folder. If file is empty DAG logs the information about it. If file is not empty it transforms data and saves it into new file.
![First DAG](https://github.com/samsdimko/task_7/blob/main/images/1_dag.png)

The second DAG detects file with updated information and loads it into MongoDB.
![Second DAG](https://github.com/samsdimko/task_7/blob/main/images/2_dag.png)

Also in ```MongoDB_queries.txt``` there are MQL queries to select such information as:
1. Top 5 the most frequent comments;
2. Each comment with less than 5 characters;
3. Average rating for each day.