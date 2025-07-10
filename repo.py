from dagster import Definitions, EnvVar, load_assets_from_modules

import assets.economist_assets as economist_assets
from jobs.jobs import (
    generate_report_job
)
from jobs.schedules import (
    generate_report_schedule
)
from resources.drivers import DuckDBResource

## A S S E T S ##
economist_asset_group = load_assets_from_modules(
    [economist_assets],  
    group_name="economist_assets"
)

all_assets = economist_asset_group  

## J O B S ##
all_jobs = [
    generate_report_job
]

## S C H E D U L E S ##
all_schedules = [
   generate_report_schedule
]

defs = Definitions(
    assets = all_assets,
    jobs=all_jobs,
    schedules=all_schedules,
    resources={
        "duckdb": DuckDBResource(
            database="data/economist.duckdb",  
        )
    }
)