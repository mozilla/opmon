# Troubleshooting

## Looker Dashboards are not showing up

Check if the dashboard has been added to the [LookML Dashboards](https://mozilla.cloud.looker.com/folders/lookml) folder and manually **move** it to the right directory.

## No data has been written to BigQuery

This can have a wide range of causes. First check if the tables are in the `operational_monitoring_derived` dataset in `mox-fx-data-shared-prod`:

* Check the logs for the latest run of [OpMon on Airflow](https://workflow.telemetry.mozilla.org/tree?dag_id=operational_monitoring)
* Check the logs in `operational_monitoring_derived.logs_v1` table
* Check the [project configuration](https://github.com/mozilla/metric-hub/tree/main/opmon)
    * Check if the `start_date` has been set to a date in the future
    * Check if configuration values are pulled in from Experimenter (this is the case for experiments and rollouts)
* Install the CLI tool and run locally
    * Debug what is going on if necessary

## Backfilling

Either run a backfill using Airflow's backfill functionality or install the CLI tooling locally and run `opmon backfill`

