from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    
    """
    Copies JSON data from S3 to staging tables in Redshift
    
    :param redshift_conn_id: Redshift connection ID
    :param aws_credentials_id: AWS credentials ID
    :param table: Table name in REDSHIFT
    :param s3_path: S3 path where JSON data resides
    :param region: AWS Region where the source data is located
    :param create_table_query: Query used to create table in REDSHIFT
    """
    
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 table = "",
                 s3_path = "",
                 region = "",
                 *args, **kwargs):
        
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_path = s3_path
        self.region = region
    
    def execute(self, context):
        self.log.info("Getting credentials")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Deleting from {self.table}")
        redshift.run("DELETE FROM {}".format(self.table))
        
        copy_sql = f"""
            COPY {self.table}
            FROM '{self.s3_path}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            REGION AS '{self.region}'
            FORMAT as JSON 's3://udacity-dend/log_json_path.json'
        """
        
        self.log.info(f"""Copying data using {copy_sql}""")
        
        redshift.run(copy_sql)





