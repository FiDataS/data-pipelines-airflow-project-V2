from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 check_stmts=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.check_stmts = check_stmts
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        #self.log.info('DataQualityOperator not implemented yet')
        self.log.info("Connect to Redshift...")
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info("Successfully connected to Redshift")
        self.log.info(f"Starting Data Quality Check with provided statements...")
        for stmt in self.check_stmts:
            self.log.info(f"checking the following statement: {stmt}")
            result = int(redshift_hook.get_first(sql=stmt['sql'])[0])
            # check if equal
            if stmt['operator'] == 'equal':
                if result != stmt['value']:
                    raise AssertionError(f"Check failed: {result} {stmt['operator']} {stmt['value']}")
            # check if not equal
            elif stmt['operator'] == 'not equal':
                if result == stmt['value']:
                    raise AssertionError(f"Check failed: {result} {stmt['operator']} {stmt['value']}")
            # check if greater than
            elif stmt['operator'] == 'greater than':
                if result <= stmt['value']:
                    raise AssertionError(f"Check failed: {result} {stmt['operator']} {stmt['value']}")
            self.log.info(f"Passed check: {result} {stmt['operator']} {stmt['value']}")