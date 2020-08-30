from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# The data quality operator, which is used to run checks on the data itself.
# The operator's main functionality is to receive one or more SQL based test
# cases along with the expected results and execute the tests.

# For each the test, the test result and expected result needs to be checked
# and if there is no match, the operator should raise an exception and the
# task should retry and fail eventually.

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 dq_check=[],
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.dq_check = dq_check
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        if len(self.dq_check) <= 0:
            self.log.info("No data quality checks provided")
            return
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        error_count = 0
        failing_tests = []
        
        for check in self.dq_check:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')

            try:
                self.log.info(f"Running query: {sql}")
                records = redshift_hook.get_records(sql)[0]
            except Exception as e:
                self.log.info(f"Query failed with exception: {e}")

            if exp_result != records[0]:
                error_count += 1
                failing_tests.append(sql)

        if error_count > 0:
            self.log.info(failing_tests)
            raise ValueError('DQ check failed')
        else:
            self.log.info("All DQ checks OK!")