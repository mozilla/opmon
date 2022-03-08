"""Generate and run the Operational Monitoring Queries."""

import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import attr
from google.cloud import bigquery

from .bigquery_client import BigQueryClient
from .config import MonitoringConfiguration
from .logging import LogConfiguration

PATH = Path(os.path.dirname(__file__))

QUERY_FILENAME = "{}_query{}.sql"
INIT_FILENAME = "{}_init.sql"
VIEW_FILENAME = "{}_view.sql"
BUCKET_NAME = "operational_monitoring"
PROJECTS_FOLDER = "projects/"
OUTPUT_DIR = "sql/moz-fx-data-shared-prod/"
PROD_PROJECT = "moz-fx-data-shared-prod"
DEFAULT_DATASET = "operational_monitoring_derived"

DATA_TYPES = {"histogram", "scalar"}

# See https://github.com/mozilla/glam/issues/1575#issuecomment-946880387
# for reference of where these numbers come from.
USERS_PER_BUILD_THRESHOLDS = {"nightly": 375, "beta": 9000, "release": 625000}

# This is a mapping of project slug to metadata.
om_projects = {}


@attr.s(auto_attribs=True)
class Monitoring:
    """Wrapper for analysing experiments."""

    project: str
    dataset: str
    config: MonitoringConfiguration
    log_config: Optional[LogConfiguration] = None

    @property
    def bigquery(self):
        return BigQueryClient(project=self.project, dataset=self.dataset)

    def run(self, submission_date):
        for data_type in DATA_TYPES:
            self._run_sql_for_data_type(submission_date, data_type)

    def _generate_sql(self):
        render_kwargs = {
            "header": "-- Generated via opmon\n",
            "gcp_project": self.project,
            "dataset": self.dataset,
        }

        if not (self.config.branches or self.config.boolean_pref):
            raise ValueError("Either branches or boolean_pref need to be defined")

        render_kwargs.update(
            {
                "branches": self.config.branches,
                "channel": self.config.channel,
                "user_count_threshold": USERS_PER_BUILD_THRESHOLDS[self.config.channel],
                "pref": self.config.boolean_pref,
                "xaxis": self.config.xaxis,
                "start_date": self.config.start_date,
            }
        )

        # todo:
        # xaxis metadata to be used to decide whether the entire table is replaced
        # Or just a partition.
        #
        # Note: there is a subtle design here in which date partitions are replaced
        # if the data is for a build over build analysis but the entire table is
        # replaced if it's a submission date analysis.

        # Iterating over each dataset to query for a given project.
        # todo: depends on config
        # for query_id, query in enumerate(om_project["analysis"]):
        #     render_kwargs.update(
        #         {"dimensions": _get_name_and_sql(query, dimensions, "dimensions")}
        #     )
        #     for data_type in DATA_TYPES:
        #         _write_sql_for_data_type(
        #             query_id,
        #             query,
        #             project,
        #             dataset,
        #             om_project["slug"],
        #             render_kwargs,
        #             probes,
        #             data_type,
        #         )

    def _sql_for_data_type(self, data_type):
        render_kwargs = {
            "header": "-- Generated via opmon\n",
            "gcp_project": self.project,
            "dataset": self.dataset,
        }

        if not (self.config.branches or self.config.boolean_pref):
            raise ValueError("Either branches or boolean_pref need to be defined")

        render_kwargs.update(
            {
                "branches": self.config.branches,
                "channel": self.config.channel,
                "user_count_threshold": USERS_PER_BUILD_THRESHOLDS[self.config.channel],
                "pref": self.config.boolean_pref,
                "xaxis": self.config.xaxis,
                "start_date": self.config.start_date,
            }
        )

        probes = self.config.probes
        probes = [probe for probe in probes if probe.data_type == data_type]

        if len(probes) == 0:
            # There are no probes for this data source + data type combo
            return None

        normalized_slug = self.slug
        render_kwargs.update(
            {
                "data_sources": self.config.data_sources,
                "probes": probes,
                "slug": self.slug,
            }
        )

        # _write_sql(
        #     project,
        #     dataset,
        #     normalized_slug,
        #     render_kwargs,
        #     QUERY_FILENAME.format(data_type, ""),
        #     QUERY_FILENAME.format(data_type, query_id),
        # )

        # if query_id > 0:
        #     # We only need to write the view/init files for the first query
        #     # (query_id == 0). The same view/table will be reused for subsequent
        #     # queries coming from a different data source.
        #     return

        # # Init and view files need the normalized slug
        # render_kwargs.update({"slug": normalized_slug})
        # _write_sql(
        #     project,
        #     dataset,
        #     normalized_slug,
        #     render_kwargs,
        #     INIT_FILENAME.format(data_type),
        #     init=True,
        # )
        # _write_sql(
        #     project,
        #     dataset,
        #     normalized_slug,
        #     render_kwargs,
        #     VIEW_FILENAME.format(data_type),
        # )

    def _run_sql_for_data_type(self, submission_date: datetime, data_type: str):
        bq_client = bigquery.Client(project=PROD_PROJECT)
        normalized_slug = self.config.slug
        destination_table = f"{self.project}.{self.dataset}.{normalized_slug}_{data_type}"
        date_partition = str(submission_date).replace("-", "")

        if self.config.xaxis == "build_id":
            destination_table += f"${date_partition}"

        query_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("submission_date", "DATE", str(submission_date)),
            ],
            use_legacy_sql=False,
            clustering_fields=["build_id"],
            default_dataset=f"{self.project}.{self.dataset}",
            time_partitioning=bigquery.TimePartitioning(field="submission_date"),
            use_query_cache=True,
            allow_large_results=True,
            write_disposition="WRITE_TRUNCATE",
            destination=destination_table,
            schema_update_options=bigquery.job.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        )

        # todo: move sql_for data_type into this

        # init_sql_path = Path(
        #     os.path.join(
        #         OUTPUT_DIR, dataset, normalized_slug, INIT_FILENAME.format(data_type)
        #     )
        # )
        # query_sql_path = Path(
        #     os.path.join(
        #         OUTPUT_DIR, dataset, normalized_slug, QUERY_FILENAME.format(data_type, "*")
        #     )
        # )
        # view_sql_path = Path(
        #     os.path.join(
        #         OUTPUT_DIR, dataset, normalized_slug, VIEW_FILENAME.format(data_type)
        #     )
        # )
        # init_query_text = init_sql_path.read_text()
        # view_text = view_sql_path.read_text()

        # # Wait for init to complete before running queries
        # init_query_job = bq_client.query(init_query_text)
        # view_query_job = bq_client.query(view_text)
        # results = init_query_job.result()

        # query_files = glob.glob(str(query_sql_path))
        # for file_id, query_file in enumerate(query_files):
        #     query_text = Path(query_file).read_text()
        #     if file_id > 0:
        #         # All subsequent files should append their output to the existing table
        #         query_config.write_disposition = "WRITE_APPEND"

        #     query_job = bq_client.query(query_text, job_config=query_config)

        #     # Periodically print so airflow gke operator doesn't think task is dead
        #     elapsed = 0
        #     while not query_job.done():
        #         time.sleep(10)
        #         elapsed += 10
        #         if elapsed % 200 == 10:
        #             print("Waiting on query...")

        #     print(f"Total elapsed: approximately {elapsed} seconds")
        #     results = query_job.result()

        #     print(f"Query job {query_job.job_id} finished")
        #     print(f"{results.total_rows} rows in {query_config.destination}")

        # # Add a view once the derived table is generated.
        # view_query_job.result()
