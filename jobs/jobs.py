from dagster import AssetSelection, define_asset_job

import assets.economist_assets as economist_assets


generate_report_job = define_asset_job(
    "generate report",
    selection=AssetSelection.assets(
        economist_assets.scrape_ssb_data,
        economist_assets.interpret_ssb_data,
        economist_assets.scrape_ppr_nb,
        economist_assets.finish_report
    ),
    description="Generates report"
)

