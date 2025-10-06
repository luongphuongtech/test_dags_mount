from airflow.sdk import BaseOperator
from airflow.plugins_manager import AirflowPlugin
class HelloOperator(BaseOperator):
    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context):
        message = f"Hello {self.name}"
        print(message)
        return message
class TrinoTestOperator(AirflowPlugin):
    name = "trino_test_operator"
    operators = [HelloOperator]