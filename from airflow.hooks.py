from airflow.hooks.base import BaseHook
from pymongo import MongoClient

class MongoDBHook(BaseHook):
    def __init__(self, mongo_conn_id='mongo_default'):
        super().__init__()
        self.mongo_conn_id = mongo_conn_id

    def get_conn(self):
        conn = self.get_connection(self.mongo_conn_id)
        client = MongoClient(conn.host, conn.port)
        return client