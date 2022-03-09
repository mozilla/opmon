"""Generate and run the Operational Monitoring Queries."""

import os
from asyncio.log import logger
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import attr
from jinja2 import Environment, FileSystemLoader

from . import Channel
from .bigquery_client import BigQueryClient
from .config import MonitoringConfiguration
from .logging import LogConfiguration
from .utils import bq_normalize_name

PATH = Path(os.path.dirname(__file__))

QUERY_FILENAME = "{}_query.sql"
VIEW_FILENAME = "{}_view.sql"
TEMPLATE_FOLDER = PATH / "templates"
DATA_TYPES = {"histogram", "scalar"}  # todo: enum

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
        destination_table = f"{self.project}.{self.dataset}.{self.normalized_slug}_{data_type}"
        date_partition = str(submission_date).replace("-", "")

        if self.config.xaxis == "build_id":
            destination_table += f"${date_partition}"

        if not (
            self.config.project.population.branches or self.config.project.population.boolean_pref
        ):
            raise ValueError("Either branches or boolean_pref need to be defined")

        probes = self.config.probes
        probes = [probe for probe in probes if probe.data_type == data_type]

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

        render_kwargs = {
            "header": "-- Generated via opmon\n",
            "gcp_project": self.project,
            "submission_date": submission_date,
            "dataset": self.dataset,
            "branches": self.config.project.population.branches,
            "channel": str(self.config.project.population.channel),
            "user_count_threshold": USERS_PER_BUILD_THRESHOLDS[
                self.config.project.population.channel
            ],
            "pref": self.config.project.population.boolean_pref
            if self.config.project.population.branches == []
            else None,
            "xaxis": str(self.config.project.xaxis),
            "start_date": self.config.project.start_date.strftime("%Y-%m-%d"),
            "data_source": self.config.project.population.data_source,
            "probes": probes,
            "slug": self.slug,
        }

        sql_filename = QUERY_FILENAME.format(data_type)
        sql = self._render_sql(sql_filename, render_kwargs)
        self.bigquery.execute(
            sql, destination_table, clustering=["build_id"], time_partitioning="submission_date"
        )
        self._publish_view(data_type)

    def _render_sql(self, template_file: str, render_kwargs: Dict[str, Any]):
        file_loader = FileSystemLoader(TEMPLATE_FOLDER)
        env = Environment(loader=file_loader)
        template = env.get_template(template_file)
        sql = template.render(**render_kwargs)
        return sql

    def _publish_view(self, data_type: str):
        sql_filename = VIEW_FILENAME.format(data_type)
        render_kwargs = {
            "gcp_project": self.project,
            "dataset": self.dataset,
            "slug": self.slug,
        }
        sql = self._render_sql(sql_filename, render_kwargs)
        self.bigquery.execute(sql)
