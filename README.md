# Sparkify Data Pipeline with Airflow

## Overview
This project is part of the Udacity Data Engineering Nanodegree program and is based on fictional data from a company called "Sparkify." The goal of this project is to create an ETL (Extract, Transform, Load) data pipeline using Apache Airflow. The data pipeline loads data from Amazon S3 into Amazon Redshift, performs SQL transformations, and conducts quality checks to ensure the integrity of the data.

## Project Structure
The project consists of the following components:

1.  dags/run_elt_pipeline_dag.py: 
The main Airflow DAG (Directed Acyclic Graph) definition file. This file defines the sequence of tasks and their dependencies.

2. plugins/operators:
- **stage_redshift.py**: Custom Airflow operator for loading data from S3 to Redshift.
- **load_fact.py**: Custom Airflow operator for loading fact data into Redshift.
- **load_dimension.py**: Custom Airflow operator for loading dimension data into Redshift.
- **data_quality.py**: Custom Airflow operator for data quality checks.

3. Queries:
- sql_cmnds/final_project_sql_statements.py: SQL queries used for data transformation and quality checks.
- sql_cmnds/create_tables.py: SQL script to create Redshift tables.

4. Image of the final DAG (*dag_image.png*)

## Setting up the Environment
Before running the DAG, make sure to set up your environment with the following steps:

1. Create an Amazon Redshift cluster.
2. Configure your Airflow environment.
3. Create an IAM role for Redshift with the necessary S3 access.
4. Store your AWS and Redshift credentials in Airflow Connections.

## Running the DAG
To run the Sparkify data pipeline, follow these steps:

1. Copy the *dags/run_elt_pipeline_dag.py* file to the *dags* directory in your Airflow installation.
2. Ensure the necessary Airflow plugins (*plugins/operators* and *sql_cmds/final_project_sql_statements*) are also in their respective directories.
3. Start the Airflow web server and scheduler.

After you've set up your environment and copied the DAG, you can trigger it to begin the ETL process.

## DAG Execution Steps
The Sparkify DAG executes the following steps:

1. Begin execution with the Begin_execution operator.
2. Load data from S3 into staging tables using the StageToRedshift operator.
3. Load fact and dimension tables using the LoadFactOperator and LoadDimensionOperator.
4. Perform data quality checks using the DataQualityOperator.
5. Complete the execution with the End_execution operator.

## Data Quality Checks
The data quality checks are designed to ensure that the data loaded into Redshift is of high quality. The checks include (for now)

- Checking if data is present
You can configure and customize these checks in the DataQualityOperator in the DAG file.

## Custom Airflow Operators
This project utilizes custom Airflow operators for various ETL tasks, such as data staging, loading fact and dimension data, and data quality checks. These operators are defined in the *plugins/operators* directory.

## SQL Transformation Queries
The SQL queries used for data transformation are stored in the plugins/helpers/sql_queries.py file. You can modify these queries as needed to match the specific requirements of your data pipeline.

## Credits
This project is based on the Udacity Data Engineering Nanodegree program and uses fictional data from the company "Sparkify."

## Author
Ilker Yenice

Feel free to reach out if you have any questions or need assistance with the Sparkify data pipeline with Airflow. Enjoy data engineering!