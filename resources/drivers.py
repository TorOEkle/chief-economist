from dagster import ConfigurableResource
import duckdb
from neo4j import GraphDatabase


class Neo4jDriver(ConfigurableResource):
    uri: str
    user: str
    password: str

    def get_driver(self):
        return GraphDatabase.driver(self.uri, auth=(self.user, self.password))
    
class DuckDBResource(ConfigurableResource):
    database: str

    def get_connection(self):
        return duckdb.connect(self.database)