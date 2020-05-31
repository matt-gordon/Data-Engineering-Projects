from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 create_table='', 
                 table='',
                 load_query='',
                 truncate_mode='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.create_table=create_table
        self.table=table
        self.load_query=load_query
        self.truncate_mode=truncate_mode

    def execute(self, context):
        ## Create Connection Hooks
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_mode == True:
            self.log.info("Clearing {} table".format(self.table))
            redshift.run("DROP TABLE IF EXISTS {}".format(self.table))
            
        ## Create Dimension table if it doesn't already exists
        self.log.info("Create {} table if it doesn't already exists".format(self.table))
        redshift.run(self.create_table)
        
        ## Load the Dimension table data from the staging_tables based upon the load query
        self.log.info("Loading data from staging tables into {}".format(self.table))
        redshift.run("INSERT INTO {} {}".format(self.table,self.load_query))
