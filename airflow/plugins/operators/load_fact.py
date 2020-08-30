from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# With fact operator, you can utilize the provided SQL helper class to run data
# transformations. Most of the logic is within the SQL transformations and the
# operator is expected to take as input a SQL statement and target database on
# which to run the query against. You can also define a target table that will
# contain the results of the transformation.


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    load_fact_sql ="""
        INSERT INTO {}
        {}
    """

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                table="",
                sql_stmt="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id= redshift_conn_id
        self.table= table   
        self.sql_stmt=sql_stmt

    def execute(self, context):
        redshift= PostgresHook(postgres_conn_id=self.redshift_conn_id)

        formatted_sql = LoadFactOperator.load_fact_sql.format(
            self.table,
            self.sql_stmt
        )

        self.log.info(f"Loading fact table '{self.table}' into Redshift")
        redshift.run(formatted_sql)
