from airflow.sdk import BaseOperator
from trino_oauth2_hook import TrinoOAuth2Hook
from airflow.plugins_manager import AirflowPlugin
class TrinoOAuth2Operator(BaseOperator):
    """
    Operator để chạy SQL trên Trino sử dụng OAuth2
    """

    def __init__(self, sql, trino_conn_id="trino_default_oauth2", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.trino_conn_id = trino_conn_id

    def execute(self, context):
        
        hook = TrinoOAuth2Hook(trino_conn_id=self.trino_conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(self.sql)
        rows = cursor.fetchall()
        self.log.info("Query result: %s", rows)
        return rows

class TrinoOAuth2OperatorPlugin(AirflowPlugin):
    name = "trino_oauth2_operator"
    operators = [TrinoOAuth2Operator]