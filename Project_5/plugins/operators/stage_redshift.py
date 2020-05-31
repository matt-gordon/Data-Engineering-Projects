from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.exceptions import AirflowException

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 s3_bucket='',
                 s3_key='',
                 table='',
                 create_table='',
                 format='',
                 clean_data='',
                 cleaning_query='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)        
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.create_table = create_table
        self.format = format
        self.clean_data=clean_data
        self.cleaning_query=cleaning_query

    def execute(self, context):
        ## Create connection hooks
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        ## Clear staging tables if they exist
        self.log.info("Clearing Redshift Staging Tables")
        redshift.run("DROP TABLE IF EXISTS {}".format(self.table))
        redshift.run(self.create_table)
        
        ## Copy data from S3 Bucket to Redshift
        self.log.info("Copying data from S3 Bucket to Redshift")
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        redshift.run("COPY {} FROM '{}' ACCESS_KEY_ID '{}' SECRET_ACCESS_KEY '{}' FORMAT AS {}".format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.format
        ))
        
        ## If clean_data flagged as True, run provided data cleaning scripts
        if self.clean_data == True:
            # Check that a cleaning query has been set as a param
            if len(self.cleaning_query) != 0:
                redshift.run(self.cleaning_query)
            else:
                raise AirflowException("Cannot clean data without setting cleaning_query param")
                
            





