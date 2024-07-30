# SparkSQL-Dataframe-Datasets

## Overview

This repository contains several projects I worked on as part of my learning journey with the Udemy course [Taming Big Data with Apache Spark - Hands On!](https://www.udemy.com/course/taming-big-data-with-apache-spark-hands-on/?couponCode=JUST4U02223). These projects showcase my skills in Apache Spark, SparkSQL, and DataFrame operations, demonstrating how to process and analyze large datasets efficiently.

## Projects

### 1. Friends by Age
**File:** `friends-by-age-dataframe.py`

This project analyzes friendship data to understand social connections across different age groups using Spark DataFrames.

**Key Steps:**
- Load the dataset with a schema.
- Select relevant columns.
- Group by age and calculate average number of friends.

**Skills Gained:**
- Loading and processing data with Spark DataFrames.
- Grouping and aggregating data.

### 2. Word Count
**File:** `word-count-better-sorted-dataframe.py`

This project implements a word count program to process and analyze text data using SparkSQL.

**Key Steps:**
- Load text data into a DataFrame.
- Split text into words using regular expressions.
- Normalize words to lowercase.
- Count the occurrences of each word.
- Sort and display the results.

**Skills Gained:**
- Text processing with Spark DataFrames.
- Using SQL functions for data transformation.

### 3. Minimum Temperature
**File:** `min-temperatures-dataframe.py`

This project finds the minimum temperatures recorded at different weather stations using a custom schema.

**Key Steps:**
- Load the temperature data with a schema.
- Filter out non-TMIN entries.
- Calculate the minimum temperature for each station.
- Convert temperatures to Fahrenheit and display the results.

**Skills Gained:**
- Working with structured data using custom schemas.
- Data filtering and aggregation.

### 4. Total Spent by Customer
**File:** `total-spent-customer-sorted-dataframe.py`

This project calculates and sorts the total amount spent by customers using transaction data.

**Key Steps:**
- Load the customer orders data with a schema.
- Group by customer ID and calculate the total amount spent.
- Sort and display the results.

**Skills Gained:**
- Grouping and aggregating transaction data.
- Sorting and displaying results efficiently.

### 5. Spark SQL
**File:** `spark-sql.py`

This project demonstrates executing SQL commands and SQL-style functions on a DataFrame.

**Key Steps:**
- Create a SparkSession.
- Load data into a DataFrame.
- Register the DataFrame as a temporary SQL table.
- Execute SQL queries to filter, group, and transform the data.

**Skills Gained:**
- Integrating SQL with Spark DataFrames.
- Executing complex SQL queries.

### 6. Spark SQL with DataFrames
**File:** `spark-sql-dataframe.py`

This project demonstrates the use of DataFrames instead of RDDs for SQL operations.

**Key Steps:**
- Create a SparkSession.
- Load data into a DataFrame.
- Perform SQL operations using DataFrame API.
- Group by and aggregate data.
- Sort and display the results.

**Skills Gained:**
- Using DataFrame API for SQL operations.
- Efficient data manipulation and transformation.
