# Chief economist Report
This repository is designed to experiment with various frameworks for agentic AI. As an initial baseline, it leverages Dagster, an orchestration tool for Python functions, to manage and automate workflows efficiently. Using Dagster provides benefits such as improved pipeline reliability, easier debugging, and better observability of data processes.

## Report

See the [final report](report_final.md) for description of logic and results.

### DuckDB Schema Creation

Navigate to the `data`folder and run the following command to create the DuckDB database.

```bash
duckdb economist.duckdb < schema.sql    
```

### Check compability of packages
To check the compatibility of the packages in the `requirements.txt` file, you can use the `uv pip compile requirements.txt` command. This command will verify that all installed packages have compatible dependencies.


