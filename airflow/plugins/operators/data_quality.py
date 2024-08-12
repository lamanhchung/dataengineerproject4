from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id = '',
                 checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks
        
    def execute(self, context):
        postgres = PostgresHook(postgres_conn_id = self.redshift_conn_id)   
        for check in self.checks:
            sql = check.get("sql")
            result = check.get("result")
            
            records = postgres.get_records(sql)[0]
            if records[0] == result:
                self.log.info("Data quality check passed")
            else:
                raise ValueError("Data quality check failed: expect {}, provided {}".format(result, records[0]))

        self.log.info("Data quality check finished")