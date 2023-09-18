Operational Monitoring
===

Operational Monitoring (OpMon) is a self-service tool that aggregates and summarizes operational metrics that indicate the health of software. OpMon can be used to continuously monitor rollouts, experiments (including experiments with continuous enrollments) or the population of a specific product (for example, Firefox Desktop).

For more information on how to set up an Operational Monitoring project, see the [documentation on dtmo](https://docs.telemetry.mozilla.org/cookbooks/operational_monitoring.html).

## Local installation

```
# Create and activate a python virtual environment.
python3 -m venv venv/
source venv/bin/activate
pip install -r requirements.txt
pip install .
```

The `opmon` CLI tool will be available to run locally:

```
$ opmon --help
Usage: opmon [OPTIONS] COMMAND [ARGS]...

  Initialize CLI.

Options:
  --log_project_id, --log-project-id TEXT
                                  GCP project to write logs to
  --log_dataset_id, --log-dataset-id TEXT
                                  Dataset to write logs to
  --log_table_id, --log-table-id TEXT
                                  Table to write logs to
  --log_to_bigquery, --log-to-bigquery
  --help                          Show this message and exit.

Commands:
  backfill         Backfill a specific project.
  preview          Create a preview for a specific project based on a subset of data.
  run              Execute the monitoring ETL for a specific date.
  validate_config  Validate config files.
```

## Documentation

User documentation is available [on dtmo](https://docs.telemetry.mozilla.org/cookbooks/operational_monitoring.html).
Developer documentation is available in the [`docs/`](https://github.com/mozilla/opmon/tree/main/docs) directory.
