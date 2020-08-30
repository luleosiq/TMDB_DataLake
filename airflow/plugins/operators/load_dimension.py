from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# With dimension operator, utilize the provided SQL helper class to run data
# transformations. Most of the logic is within the SQL transformations and the
# operator is expected to take as input a SQL statement and target database on
# which to run the query against. You can also define a target table that will
# contain the results of the transformation.

# Dimension loads are often done with the truncate-insert pattern where the
# target table is emptied before the load.

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    insert_dim_sql = """
        INSERT INTO {}
        {};
    """

    truncate_dim_sql = """
        TRUNCATE TABLE {};
    """


    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                table="",
                sql_stmt="",
                truncate=False,
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id= redshift_conn_id
        self.table= table
        self.sql_stmt=sql_stmt
        self.truncate=truncate


    def execute(self, context):
        redshift= PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate:
            self.log.info(f"Truncating dimension table: {self.table}")
            redshift.run(LoadDimensionOperator.truncate_dim_sql.format(self.table))

        self.log.info(f"Loading dimension table {self.table}")
        formatted_sql = LoadDimensionOperator.insert_dim_sql.format(
            self.table,
            self.sql_stmt
        )

        self.log.info(f"Executing Query: {formatted_sql}")
        redshift.run(formatted_sql)
