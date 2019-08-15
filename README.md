# Data-Modeling-with-Postgres

## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Description
In this project, you'll apply what you've learned on data modeling with Postgres and build an ETL pipeline using Python. To complete the project, you will need to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

## explanation of files in the repository
<ul>
<li>create_tables.py - a python script that will do the following:
    <ul>
        <li>drop and create the sparkify database</li>
        <li>drop tables as indicated in the sql_queries.py script</li>
        <li>create tables as indicated in the sql_queries.py script</li>
    </ul>
</li>    
<li>etl.ipynb - a jupyter python notebook used for extracting data from both a song data directory and a log data directory
    <ul>
        <li>the data is then transformed and loaded into applicable dimension and fact tables</li>
    </ul>
</li>    
<li>etl.py - a python script that will perform the extracting, transforming and loading of data</li>
<li>README.md - this file</li>
<li>sql_queries.py - a python script that contains sql queries that perform various functions such as:
    <ul>
        <li>drop tables</li>
        <li>create tables</li>
        <li>insert into tables</li>
        <li>select from tables</li>
    </ul>
</li>
<li>test.ipynb - a jupyter python notebook used for querying the fact and dimension tables</li>
</ul>

## how to run the Python scripts
prerequisites to running the python scripts locally i installed the following:
<ul>
<li>Anaconda3 (64 bit) - this will install python, jupyter and spyder</li>    
<li>PostgreSQL 11</li>    
</ul>
After installing the above and doing a few administrative things I execute the python scripts as follows:
execute an Anaconda Prompt
navigate to the folder where the python scripts are located i.e. D:\Documents\GitHub\Udacity\Data Engineering Nanodegree\Data-Modeling-with-Postgres
<br>
Type the following and press enter:
    <ul>
        <li>py create_tables.py</li>
        <li>py etl.py</li>
    </ul>
<br>
Be forewarned I have left a lot of print statements in the python scripts for debugging purposes - I may add a debug switch at a later date

## etl.py
I downloaded the data folder from the Project Workspace onto a Windows 10 machine into the following folder:
D:\data
I added a windows boolean indicator in the main function at the end of the etl.py file. This windows boolean indicator has been set to False which means that it will process data from the Project Workspace and not from the local Windows 10 machine.
Also of note, the etl.py can be run more than once. It should not complain about rows already being in existence, the insert scripts have been coded to ignore primary key conflicts. 

## star schema
Since we are dealing with dimensions and a fact table I have added the applicable foreign key constraints to the songplays table.
By doing so we now have a true star schema associated with song plays.
I have updated the sql_queries.py script to reflect the changes to the songplays table.