from dagster import ScheduleDefinition

from jobs.jobs import generate_report_job


# Define schedules for the jobs
generate_report_schedule = ScheduleDefinition(
    job=generate_report_job,
    cron_schedule="0 1 16 * *",  # Runs daily at midnight
    execution_timezone="Europe/Oslo",
    description="Monthly job to generate report based on latest data"
)
