from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue
from resources.drivers import DuckDBResource
from resources.utils import Logg, measure_time_with_metadata
from functions.macroeconomics import transform_build_index, plot_cost_index, get_latest_ppr_url
import json
import requests
from pyjstat import pyjstat
import pandas as pd
import matplotlib.pyplot as plt
import os
from datetime import datetime
from bs4 import BeautifulSoup
from openai import OpenAI
from pathlib import Path



@asset( kinds={"duckdb"})
@measure_time_with_metadata
def scrape_ssb_data(context: AssetExecutionContext,
                  duckdb: DuckDBResource) -> MaterializeResult:
    """
    Scrape data from SSB,transform and write it to DuckDB.
    """
    SSB_URL = 'https://data.ssb.no/api/v0/no/table/08651/'
    query_path = Path(__file__).parent / "queries" / "build_index.json"
    with open(query_path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    # Make POST request to SSB API
    response = requests.post(SSB_URL, json=payload)

    if response.status_code == 200:
        dataset = pyjstat.Dataset.read(response.text)
        df = dataset.write('dataframe')
        Logg.info(context,f"Retrieved {df.shape[0]} rows of data")
    else:
        Logg.info(context,f"Request failed with status code {response.status_code}")

    df = transform_build_index(df)
    output_path = Path(__file__).parent.parent / "cost_index.png"
    plot_base64 = plot_cost_index(df, output_path)
    # Write data to duckdb
    with duckdb.get_connection() as conn:
            conn.execute("""
                    INSERT INTO build_index (input_factor, month, cost_index, delta_month_pct, delta_year_pct)
                    SELECT input_factor, month, cost_index, delta_month_pct, delta_year_pct
                    FROM df
                         """)
            Logg.info(context, "Data written to DuckDB table 'build_index'.")
            conn.close()

    return MaterializeResult(
        metadata={
            "num_rows": MetadataValue.int(len(df)),
            "num_columns": MetadataValue.int(len(df.columns)),
            "columns": MetadataValue.json({"columns": list(df.columns)}),
            "sample_data":  MetadataValue.md(df.head().to_markdown()),
            "cost_index": MetadataValue.md(f"![Cost Index](data:image/png;base64,{plot_base64})")
        }
    )

@asset( 
        kinds={"duckdb"},
        deps=[scrape_ssb_data]
       )
@measure_time_with_metadata
def interpret_ssb_data(context: AssetExecutionContext,
                  duckdb: DuckDBResource) -> MaterializeResult:
    """
    Pass along the 13 last observations from the SSB data to chatGPT for intepretation 
    """
    with duckdb.get_connection() as conn:
        df = conn.execute("SELECT * FROM build_index").fetchdf()
        Logg.info(context, f"Read {len(df)} rows from 'abuild_index'.")
    
    labour_series = (
        df[df["input_factor"] == "labour"]
        .sort_values("month")
        .set_index("month")["delta_year_pct"]
        .tail(13)
        .round(2)
        .to_dict()
    )
    materials_series = (
        df[df["input_factor"] == "materials"]
        .sort_values("month")
        .set_index("month")["delta_year_pct"]
        .tail(13)
        .round(2)
        .to_dict()
    )
    client = OpenAI(api_key=os.getenv("ECONOMIST_KEY"))
    messages = [
        {
            "role": "system",
            "content": "You are a confident chief economist with clear communication and deep macroeconomic insight."
        },
        {
            "role": "user",
            "content": (
                "Imagine you are a chief economist with strong self-confidence and presence. You are not afraid of beeing a strait shooter."
                "Use these two time series to provide a brief commentary on the current macroeconomic picture in Norway.\n\n"
                "Materials reflect international economic conditions, while labour reflects domestic conditions.\n\n"
                "Also explain briefly how these developments may affect the interest rate decisions of Norges Bank.They target a 2% year over year price increase\n\n"
                f"here is the fime series for materials. this is delta year over year in percentage {materials_series}"
                f"here is the time series for labour this is delta year over year in percentage{labour_series}"
                "reply in markdown format. Go straight to your report"
            )
        }
    ]

    # Create a chat completion
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=messages,
        temperature=0.7,
        max_tokens=1000
    )

    first_report = response.choices[0].message.content
    # Store the report in a text file for later use
    data_path = Path("../data")
    summary_path = data_path / "first_report.txt"
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(first_report)   

    Logg.info(context, f"Insert first report into report_template")
    # Insert initial report into the template
    template_path = Path(__file__).parent.parent / "report_template.md"
    output_path = Path(__file__).parent.parent / "report1.md"

    with open(template_path, "r", encoding="utf-8") as f:
        template = f.read()

    placeholder = "<!-- INSERT_CHIEF_ECONOMIST_REPORT_HERE -->"
    insertion = f"\n\n#### Report generated {datetime.now():%Y-%m-%d %H:%M}\n\n{first_report}\n\n{placeholder}"

    filled_report = template.replace(placeholder, insertion)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(filled_report)

    return MaterializeResult(
        metadata={
            "num_rows": MetadataValue.int(len(df)),
            "first_report":  MetadataValue.md(first_report)
        }
    )


@asset()
@measure_time_with_metadata
def scrape_ppr_nb(context: AssetExecutionContext,
                  ) -> MaterializeResult:
    """
    Scrape summary from last PPR form Norges bank.
    Store text as a txt file.
    """

    latest_url = get_latest_ppr_url()
    Logg.info(context, f"Latest PPR URL: {latest_url}")
    if not latest_url:
        raise ValueError("Could not find the latest PPR URL.")

    # Send GET request
    response = requests.get(latest_url)
    response.raise_for_status()  

    # Parse content
    soup = BeautifulSoup(response.text, "html.parser")
    section = soup.find("section", class_="publication-chapter__chapter", id="1")

    if not section:
       raise ValueError("Could not find <section> with id='1' and class='publication-chapter__chapter'")

    paragraphs = section.find_all("p")
    ppr_summary = "\n\n".join(p.get_text(strip=True) for p in paragraphs)

    # save summary as txt to data folder
    data_path = Path("../data")
    summary_path = data_path / "ppr_summary.txt"
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(ppr_summary)    
    
    
    return MaterializeResult(
        metadata={
            "ppr_summary": MetadataValue.md(ppr_summary)
        }
    )

@asset(deps=[scrape_ppr_nb, interpret_ssb_data])
@measure_time_with_metadata
def finish_report(context: AssetExecutionContext,
                  ) -> MaterializeResult:
    """
    Finalize the report by correcting for input from latest PPR.
    """
    data_path = Path(__file__).resolve().parent.parent / "data"
    ppr_summary_path = data_path / "ppr_summary.txt"
    first_report_path = data_path / "first_report.txt"  
    with open(ppr_summary_path, "r", encoding="utf-8") as f:
        ppr_summary = f.read()
    with open(first_report_path, "r", encoding="utf-8") as f:
        first_report = f.read()

    client = OpenAI(api_key=os.getenv("ECONOMIST_KEY"))

    # Prepare prompt with injected prior reports
    messages = [
        {
            "role": "system",
            "content": (
                "You are a confident chief economist with clear communication, presence, and deep macroeconomic insight. "
                "You can synthesize official publications with internal analysis into a clear and concise narrative."
            )
        },
        {
            "role": "user",
            "content": (
                "You are preparing a final macroeconomic report for use at the broader public audience. \n\n"
                            "Below is your internal preliminary assessment based on building cost indices (materials and labour):\n\n"
                f"{first_report}\n\n"
                "Below is a summary from the latest official Pengepolitisk Rapport which is written by very good economist:\n\n"
                f"{ppr_summary}\n\n"
                "---\n\n"
                "Now write a **final report** where you take the summary from norges bank in hindsight, give it in markdown format. "
                "Include a short opening paragraph, make it sound like you was always right, then use subheadings where appropriate. "
                "Focus on clear implications for interest rate policy. Write with precision and authority."
            )
        }
    ]

    # Create chat completion
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=messages,
        temperature=0.7,
        max_tokens=2000
    )

    # Extract content and save
    final_report = response.choices[0].message.content

    final_report_path = data_path / "final_report.txt"
    with open(final_report_path, "w", encoding="utf-8") as f:
        f.write(final_report)
    repo_root = Path(__file__).resolve().parent.parent
    template_path = repo_root / "report1.md"
    output_path = repo_root / "report_final.md"

    with open(template_path, "r", encoding="utf-8") as f:
        template = f.read()

    placeholder = "<!-- INSERT_HINDSIGHT_REPORT_HERE -->"
    insertion = f"\n\n#### Report generated {datetime.now():%Y-%m-%d %H:%M}\n\n{first_report}\n\n{placeholder}"

    filled_report = template.replace(placeholder, insertion)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(filled_report)

    return MaterializeResult(
        metadata={
            "hindsight_report": MetadataValue.md(final_report)
        }
    )