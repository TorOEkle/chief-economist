import pandas as pd
import matplotlib.pyplot as plt
import pandas as pd
import requests
from datetime import datetime
import base64
import io
from pathlib import Path


def transform_build_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the raw building cost index dataframe into a clean, pivoted format.

    - Renames columns for clarity
    - Converts 'month' to datetime
    - Filters out rows before January 2015
    - Standardizes values for input factors and variables
    - Pivots the table so each variable becomes a column

    Parameters:
        df (pd.DataFrame): Raw input DataFrame from SSB

    Returns:
        pd.DataFrame: Transformed and tidy DataFrame
    """
    df = df.copy()

    # Rename columns
    df.rename(columns={
        "arbeidstype": "input_factor",
        "måned": "month"
    }, inplace=True)

    # Convert month column to datetime
    if df["month"].dtype == "object":
        df["month"] = pd.to_datetime(df["month"].str.replace("M", ""), format="%Y%m")

    # Filter from 2015 onwards
    df = df[df["month"] >= datetime(2015, 1, 1)]

    # Standardize variable names
    variable_mapping = {
        "Byggekostnadsindeks": "cost_index",
        "Endring frå førre månad (prosent)": "delta_month_pct",
        "Endring frå førre år (prosent)": "delta_year_pct"
    }
    df["statistikkvariabel"] = df["statistikkvariabel"].replace(variable_mapping)

    input_mapping = {
        "Arbeidskraft": "labour",
        "Materialar": "materials"
    }
    df["input_factor"] = df["input_factor"].replace(input_mapping)

    # Pivot the table
    df = df.pivot_table(
        index=["input_factor", "month"],
        columns="statistikkvariabel",
        values="value"
    ).reset_index()

    # Sort for readability
    df = df.sort_values(["input_factor", "month"])

    return df

def plot_cost_index(df: pd.DataFrame, output_path: str = "cost_index.png") -> str:
    """
    Plot cost index and return the base64 string for inline use in Dagster.
    
    Parameters:
    - df: DataFrame with columns ['input_factor', 'month', 'cost_index']
    - output_path: Where to save the PNG file.

    Returns:
    - Base64 string of the plot image for inline metadata.
    """
    pivot_df = df.pivot(index="month", columns="input_factor", values="cost_index")

    plt.figure(figsize=(12, 6))
    plt.plot(pivot_df.index, pivot_df["labour"], label="Labour", color="darkslateblue")
    plt.plot(pivot_df.index, pivot_df["materials"], label="Materials", color="chocolate")
    plt.text(
        pivot_df.index[-1], pivot_df["labour"].iloc[-1], 
        " Labour", va="bottom", ha="right", fontsize=10, color="darkslateblue"
    )
    plt.text(
        pivot_df.index[-1], pivot_df["materials"].iloc[-1], 
        " Materials", va="bottom", ha="right", fontsize=10, color="chocolate"
    )
    plt.title("Cost Index Over Time")
    plt.xlabel("Month")
    plt.ylabel("Cost Index")
    plt.grid(True)
    plt.tight_layout()
    plt.figtext(0.99, 0.01, "Source: SSB Table 08651", ha='right', fontsize=11, style='italic')

    # Save the figure to file
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_file, dpi=300)

    # Save to buffer for Dagster UI
    buffer = io.BytesIO()
    plt.savefig(buffer, format="png", dpi=150, bbox_inches="tight")
    buffer.seek(0)
    plot_base64 = base64.b64encode(buffer.getvalue()).decode()
    plt.close()

    return plot_base64


def get_latest_ppr_url():
    base_url = "https://www.norges-bank.no/aktuelt/publikasjoner/Pengepolitisk-rapport"
    current_year = datetime.now().year

    # Try current year first, from report 4 to 1
    for year in [current_year, current_year - 1]:
        for report_nr in range(4, 0, -1):
            url = f"{base_url}/{year}/ppr-{report_nr}{year}/nettrapport-ppr-{report_nr}{year}/"
            try:
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    return url  # Return first found valid URL
            except requests.RequestException:
                continue  # Ignore failures and try next

    raise Exception("Could not find any valid Pengepolitisk Rapport URL")
