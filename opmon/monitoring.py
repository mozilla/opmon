"""Generate and run the Operational Monitoring Queries."""

import os
from asyncio.log import logger
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import attr
from jinja2 import Environment, FileSystemLoader

from . import Channel, errors
from .bigquery_client import BigQueryClient
from .config import MonitoringConfiguration
from .dryrun import dry_run_query
from .logging import LogConfiguration
from .utils import bq_normalize_name

PATH = Path(os.path.dirname(__file__))

QUERY_FILENAME = "{}_query.sql"
VIEW_FILENAME = "{}_view.sql"
PROJECTS_FILENAME = "projects.sql"
PROJECTS_TABLE = "projects_v1"
TEMPLATE_FOLDER = PATH / "templates"
DATA_TYPES = {"histogram", "scalar"}  # todo: enum

# See https://github.com/mozilla/glam/issues/1575#issuecomment-946880387
# for reference of where these numbers come from.
# USERS_PER_BUILD_THRESHOLDS = {Channel.NIGHTLY: 375, Channel.BETA: 9000, Channel.RELEASE: 625000}
# todo: adjust thresholds
USERS_PER_BUILD_THRESHOLDS = {Channel.NIGHTLY: 1, Channel.BETA: 1, Channel.RELEASE: 1}


@attr.s(auto_attribs=True)
class Monitoring:
    """Wrapper for analysing experiments."""

    project: str
    dataset: str
    slug: str
    config: MonitoringConfiguration
    log_config: Optional[LogConfiguration] = None

    @property
    def bigquery(self):
        """Return the BigQuery client instance."""
        return BigQueryClient(project=self.project, dataset=self.dataset)

    @property
    def normalized_slug(self):
        """Return the normalized slug."""
        return bq_normalize_name(self.slug)

    def run(self, submission_date):
        """Execute and generate the operational monitoring ETL for a specific date."""
        for data_type in DATA_TYPES:
            # Periodically print so airflow gke operator doesn't think task is dead
            print(f"Run query for {self.slug} for {data_type} types")
            self._run_sql_for_data_type(submission_date, data_type)
        self._update_metadata()
        return True

    def _run_sql_for_data_type(self, submission_date: datetime, data_type: str):
        """Generate and execute the ETL for a specific data type."""
        self._check_runnable(submission_date)
        date_partition = str(submission_date).replace("-", "").split(" ")[0]
        destination_table = f"{self.normalized_slug}_{data_type}${date_partition}"

        self.bigquery.execute(
            self._get_data_type_sql(submission_date=submission_date, data_type=data_type),
            destination_table,
            clustering=["build_id"],
            time_partitioning="submission_date",
            dataset=f"{self.dataset}_derived",
        )

        print(f"Create view for {self.slug} {data_type}")
        self.bigquery.execute(self._get_data_type_view_sql(data_type=data_type))

    def _render_sql(self, template_file: str, render_kwargs: Dict[str, Any]):
        """Render and return the SQL from a template."""
        file_loader = FileSystemLoader(TEMPLATE_FOLDER)
        env = Environment(loader=file_loader)
        template = env.get_template(template_file)
        sql = template.render(**render_kwargs)
        return sql

    def _get_data_type_sql(
        self, submission_date: datetime, data_type: str, first_run: Optional[bool] = None
    ) -> str:
        """Return SQL for data_type ETL."""
        probes = self.config.probes
        probes = [probe for probe in probes if probe.type == data_type]

        if len(probes) == 0:
            # There are no probes for this data source + data type combo
            logger.warning(
                f"No probes for data type {data_type} configured for {self.slug}.",
                extra={"experiment": self.slug},
            )

        # todo:
        # xaxis metadata to be used to decide whether the entire table is replaced
        # Or just a partition.
        #
        # Note: there is a subtle design here in which date partitions are replaced
        # if the data is for a build over build analysis but the entire table is
        # replaced if it's a submission date analysis.

        # group probes that are part of the same dataset
        # necessary for creating the SQL template
        probes_per_dataset = {}
        for probe in probes:
            if probe.data_source.name not in probes_per_dataset:
                probes_per_dataset[probe.data_source.name] = [probe]
            else:
                probes_per_dataset[probe.data_source.name].append(probe)

        # check if this is the first time the queries are executed
        # the queries are referencing the destination table if build_id is used for the time frame
        if first_run is None:
            destination_table = (
                f"{self.project}.{self.dataset}_derived.{self.normalized_slug}_{data_type}"
            )
            first_run = True
            try:
                self.bigquery.client.get_table(destination_table)
                first_run = False
            except Exception:
                first_run = True

        render_kwargs = {
            "header": "-- Generated via opmon\n",
            "gcp_project": self.project,
            "submission_date": submission_date,
            "config": self.config.project,
            "dataset": self.dataset,
            "first_run": first_run,
            "dimensions": self.config.dimensions,
            # "user_count_threshold": USERS_PER_BUILD_THRESHOLDS[
            #     self.config.project.population.channel
            # ],
            "probes_per_dataset": probes_per_dataset,
            "slug": self.slug,
            "normalized_slug": self.normalized_slug,
        }

        sql_filename = QUERY_FILENAME.format(data_type)
        sql = self._render_sql(sql_filename, render_kwargs)
        return sql

    def _get_data_type_view_sql(self, data_type: str) -> str:
        """Return the SQL to create a BigQuery view."""
        sql_filename = VIEW_FILENAME.format(data_type)
        render_kwargs = {
            "gcp_project": self.project,
            "dataset": self.dataset,
            "config": self.config.project,
            "normalized_slug": self.normalized_slug,
            "dimensions": self.config.dimensions,
        }
        sql = self._render_sql(sql_filename, render_kwargs)
        return sql

    def _check_runnable(self, current_date: Optional[datetime] = None) -> bool:
        """Check whether the opmon project can be run based on configuration parameters."""
        if self.config.project is None:
            raise errors.ConfigurationException("Configuration has no project config.")

        if self.config.project.start_date is None:
            raise errors.NoStartDateException(self.slug)

        if (
            current_date
            and self.config.project.end_date
            and self.config.project.end_date < current_date
        ):
            raise errors.EndedException(self.slug)

        return True

    def validate(self) -> None:
        """Validate ETL and configs of opmon project."""
        self._check_runnable()

        for data_type in DATA_TYPES:
            data_type_sql = self._get_data_type_sql(
                submission_date=self.config.project.start_date,  # type: ignore
                data_type=data_type,
                first_run=True,
            )
            dry_run_query(data_type_sql)

    def _update_metadata(self) -> None:
        """Update the BQ table with project metadata to add/update information for this project."""
        destination_table = f"{self.project}.{self.dataset}_derived.{PROJECTS_TABLE}"

        # check if projects metadata table exists; otherwise it needs to be created
        first_run = True
        try:
            self.bigquery.client.get_table(destination_table)
            first_run = False
        except Exception:
            first_run = True

        probes = self.config.probes
        render_probes = [
            {"name": probe.name, "agg_type": probe.type} for probe in probes if probe.type
        ]

        render_kwargs = {
            "gcp_project": self.project,
            "dataset": self.dataset,
            "table": PROJECTS_TABLE,
            "config": self.config.project,
            "slug": self.slug,
            "dimensions": self.config.dimensions,
            "probes": render_probes,
            "first_run": first_run,
        }
        query = self._render_sql(PROJECTS_FILENAME, render_kwargs=render_kwargs)
        self.bigquery.execute(query)

        # Create view
        view_name = PROJECTS_TABLE.split("_")[0]
        view_query = f"""
            CREATE OR REPLACE VIEW `{self.project}.{self.dataset}.{view_name}` AS (
                SELECT *
                FROM `{self.project}.{self.dataset}_derived.{PROJECTS_TABLE}`
            )
        """

        self.bigquery.execute(view_query)
        print(f"Updated project metadata for {self.slug}")
