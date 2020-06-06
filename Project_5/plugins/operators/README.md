# Custom Operators

This section details the required variables for each custom operator:  


### StageToRedshiftOperator (source in stage_redshift.py)

task_id: Name of the task.  
dag: name of the dag this operator is being assigned to.  
aws_credentials_id: The name of the aws connection defined in Airflow.  
redshift_conn_id: The name of the redshift connection defined in Airflow.  
s3_bucket: S3 bucket name.  
s3_key S3 key name.  
create_table: SQL query defining the CREATE TABLE routine.  
table: Name of the table to be populated by operator.  
format: Definition of the format of the input data. Accepts either a json_path definition or "json 'auto'".  
clean_data: Boolean. If True, the cleaning_query needs to be defined and will be run on the created table.  
cleaning_query: SQL query defining the data cleaning routines to be run on the table.  


### LoadFactOperator (source in load_fact.py)

task_id: Name of the task.  
dag: name of the dag this operator is being assigned to.  
redshift_conn_id: The name of the redshift connection defined in Airflow.  
create_table: SQL query defining the CREATE TABLE routine.  
table: Name of the table to be populated by operator.  
load_query: SQL query defining the COPY routine to populate the table.  


### LoadDimensionOperator (source in load_dimension.py)

task_id: Name of the task.  
dag: name of the dag this operator is being assigned to.  
redshift_conn_id: The name of the redshift connection defined in Airflow.  
create_table: SQL query defining the CREATE TABLE routine.  
table: Name of the table to be populated by operator.  
load_query: SQL query defining the COPY routine to populate the table.  
truncate_mode: Boolean. If True, table is removed (if it exists) prior to loading of data, otherwise data is appended.  


### DataQualityOperator (source in data_quality.py)

task_id: Name of the task.  
dag: name of the dag this operator is being assigned to.  
redshift_conn_id: The name of the redshift connection defined in Airflow.  
test_queries: List of SQL queries defining the data quality tests to be run. The required result is to be defined in expected_results.  
expected_results: List of test results that must be the same size as the number of tests defined in test_queries.
