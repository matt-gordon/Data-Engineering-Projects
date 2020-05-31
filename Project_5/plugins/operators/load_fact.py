from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 create_table='',
                 table='',
                 load_query='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.create_table=create_table
        self.table=table
        self.load_query=load_query
       
    def execute(self, context):
        # Create Connection Hooks
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Create the Fact table if it doesn't exist
        self.log.info("Create Table {} if it doesn't already exist".format(self.table))
        redshift.run(self.create_table)
        
        # Load data from the staging table into the Fact table
        self.log.info("Loading data from staging table into {}".format(self.table))
        redshift.run("INSERT INTO {} {}".format(self.table,self.load_query))

