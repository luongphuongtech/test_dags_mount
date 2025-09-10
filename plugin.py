from airflow.plugins_manager import AirflowPlugin
from trino_oauth2_hook import TrinoOAuth2Hook


class TrinoOAuth2Plugin(AirflowPlugin):
    name = "trino_oauth2_plugin"
    hooks = [TrinoOAuth2Hook]
