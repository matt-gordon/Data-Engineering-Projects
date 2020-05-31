from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 test_queries=[],
                 expected_results=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_queries = test_queries
        self.expected_results = expected_results


    def execute(self, context):
        # Establish Connection Hooks
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Execute Tests & Compare to Expected Results
        for test in range(len(self.test_queries)):
            test_result = redshift.get_first(self.test_queries[test])
            if test_result[0] != self.expected_results[test]:
                raise ValueError('Test no. {} failed;\n {}'.format(test,self.test_queries[test]))
            else:
                self.log.info("Test no. {} passed".format(test))
            