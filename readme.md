# Chief economist Report
This repository is designed to experiment with various frameworks for agentic AI. As an initial baseline, it leverages Dagster, an orchestration tool for Python functions, to manage and automate workflows efficiently. Using Dagster provides benefits such as improved pipeline reliability, easier debugging, and better observability of data processes.

## Report

See the [final report](report_final.md) for description of logic and results.

### Preliminary report
For the first part of the report I pull the last 13 datapoints for YoY percentage change for both materials and labour and paste in the following prompt:

```
Imagine you are a chief economist with strong self-confidence and presence. You are not afraid of beeing a strait shooter.
Use these two time series to provide a brief commentary on the current macroeconomic picture in Norway.
Materials reflect international economic conditions, while labour reflects domestic conditions.
Also explain briefly how these developments may affect the interest rate decisions of Norges Bank.They target a 2% year over year price increase
here is the fime series for materials. this is delta year over year in percentage {materials_series}
here is the time series for labour this is delta year over year in percentage{labour_series}
reply in markdown format. Go straight to your report
```

In the second prompt i provide both the summary from "pengepolitisk rapport" and our initial report with the following prompt:

```
You are preparing a final macroeconomic report for use at the broader public audience. 
Below is your internal preliminary assessment based on building cost indices (materials and labour):
f{first_report}
Below is a summary from the latest official Pengepolitisk Rapport which is written by very good economist:
f{ppr_summary}

Now write a **final report** where you take the summary from norges bank in hindsight, give it in markdown format. 
Include a short opening paragraph, make it sound like you was always right, then use subheadings where appropriate. 
Focus on clear implications for interest rate policy. Write with precision and with authority
```

I use temperature 0.7 for both prompts and the model is `gpt-4o`.

### Set up

I use `uv` to set up virtual environments. It should be enough to run `setup.sh` to get everything redy before running `dagster-dev`

### Check compability of packages
To check the compatibility of the packages in the `requirements.txt` file, you can use the `uv pip compile requirements.txt` command. This command will verify that all installed packages have compatible dependencies.


