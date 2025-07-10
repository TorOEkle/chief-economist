from dagster import ConfigurableResource
import duckdb
    
class DuckDBResource(ConfigurableResource):
    database: str

    def get_connection(self):
        return duckdb.connect(self.database)