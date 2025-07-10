# Dagster

Dagster is an open-source data orchestrator for machine learning, analytics, and ETL. It helps you build, run, and monitor data pipelines with ease.
In this repo I will have an simple example of how to use Dagster for demos and connect to Neo4j.

## Features

- **Pipeline Orchestration**: Design and manage complex data workflows.
- **Data Asset Management**: Track and version your data assets.
- **Observability**: Monitor and debug your pipelines with detailed logs and metrics.
- **Extensibility**: Integrate with various data tools and platforms.


## Developer tools

### Linter - ruff with Black formatter and pre-commit hooks
Ruff is a fast Python linter that can be used with pre-commit hooks to ensure code quality and consistency. It can be configured to work with the Black formatter for automatic code formatting. This is better explained in the [ruff_beskrivning](ruff_beskrivning.md) file. **PS:** This is in swedish because it was written by my eminent colleague Jens klarèn for our team in Sweden.

### Pytest
Pytest is a testing framework for Python that makes it easy to write simple and scalable test cases. It can be used to test your Dagster pipelines and ensure they work as expected.Or to test your transformation code etc before pushing code to repo. 

## License

Dagster is licensed under the Apache 2.0 License.


### DuckDB Schema Creation

Navigate to the `data`folder and run the following command to create the DuckDB database.

```bash
duckdb economist.duckdb < schema.sql    
```

### Check compability of packages
To check the compatibility of the packages in the `requirements.txt` file, you can use the `uv pip compile requirements.txt` command. This command will verify that all installed packages have compatible dependencies.


