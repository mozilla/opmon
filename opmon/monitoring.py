"""Generate and run the Operational Monitoring Queries."""

import os
from asyncio.log import logger
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import attr
from jinja2 import Environment, FileSystemLoader

from . import Channel, MonitoringPeriod, errors
from .bigquery_client import BigQueryClient
from .config import MonitoringConfiguration
from .dryrun import dry_run_query
from .logging import LogConfiguration
from .utils import bq_normalize_name

PATH = Path(os.path.dirname(__file__))

QUERY_FILENAME = "{}_query.sql"
VIEW_FILENAME = "{}_view.sql"
TEMPLATE_FOLDER = PATH / "templates"
DATA_TYPES = {"histogram", "scalar"}  # todo: enum
DEFAULT_PARTITION_EXPIRATION = 432000000  # 5 days

# See https://github.com/mozilla/glam/issues/1575#issuecomment-946880387
# for reference of where these numbers come from.
USERS_PER_BUILD_THRESHOLDS = {Channel.NIGHTLY: 375, Channel.BETA: 9000, Channel.RELEASE: 625000}

# This is a mapping of project slug to metadata.
om_projects = {}


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
        return BigQueryClient(project=self.project, dataset=self.dataset)

    @property
    def normalized_slug(self):
        return bq_normalize_name(self.slug)

    def run(self, submission_date):
        for data_type in DATA_TYPES:
            # Periodically print so airflow gke operator doesn't think task is dead
            print(f"Run query for {self.slug} for {data_type} types")
            self._run_sql_for_data_type(submission_date, data_type)

    def _run_sql_for_data_type(self, submission_date: datetime, data_type: str):
        self.check_runnable(submission_date)

        destination_table = f"{self.project}.{self.dataset}.{self.normalized_slug}_{data_type}"
        date_partition = str(submission_date).replace("-", "")

        if self.config.xaxis == "build_id":
            destination_table += f"${date_partition}"

        partition_expiration_ms = None
        if self.config.project.xaxis != MonitoringPeriod.DAY:
            partition_expiration_ms = DEFAULT_PARTITION_EXPIRATION

        self.bigquery.execute(
            self._get_data_type_sql(submission_date=submission_date, data_type=data_type),
            destination_table,
            clustering=["build_id"],
            time_partitioning="submission_date",
            partition_expiration_ms=partition_expiration_ms,
        )
        self.bigquery.execute(self._get_data_type_view_sql(data_type=data_type))

    def _render_sql(self, template_file: str, render_kwargs: Dict[str, Any]):
        file_loader = FileSystemLoader(TEMPLATE_FOLDER)
        env = Environment(loader=file_loader)
        template = env.get_template(template_file)
        sql = template.render(**render_kwargs)
        return sql

    def _get_data_type_sql(
        self, submission_date: datetime, data_type: str, first_run: Optional[bool] = None
    ) -> str:
        """Return SQL for data_type ETL."""
        destination_table = f"{self.project}.{self.dataset}.{self.normalized_slug}_{data_type}"

        probes = self.config.probes
        probes = [probe for probe in probes if probe.type == data_type]

        if len(probes) == 0:
            # There are no probes for this data source + data type combo
            logger.warning(
                f"No probes for data type {data_type} configured for {self.slug}.",
                extra={"experiment": self.slug},
            )
            return

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
            "user_count_threshold": USERS_PER_BUILD_THRESHOLDS[
                self.config.project.population.channel
            ],
            "probes_per_dataset": probes_per_dataset,
            "slug": self.slug,
        }

        sql_filename = QUERY_FILENAME.format(data_type)
        sql = self._render_sql(sql_filename, render_kwargs)
        return sql

    def _get_data_type_view_sql(self, data_type: str) -> str:
        """Returns the SQL to create a BigQuery view."""
        sql_filename = VIEW_FILENAME.format(data_type)
        render_kwargs = {
            "gcp_project": self.project,
            "dataset": self.dataset,
            "slug": self.slug,
            "start_date": self.config.project.start_date.strftime("%Y-%m-%d"),
        }
        sql = self._render_sql(sql_filename, render_kwargs)
        return sql

    def _check_runnable(self, current_date: Optional[datetime] = None) -> bool:
        """Checks whether the opmon project can be run based on configuration parameters."""
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
                submission_date=self.config.project.start_date, data_type=data_type, first_run=True
            )
            print(data_type_sql)
            dry_run_query(data_type_sql)
