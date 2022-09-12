"""Generate and run the Operational Monitoring Queries."""

import os
from asyncio.log import logger
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import attr
from jinja2 import Environment, FileSystemLoader

from . import AlertType, errors
from .bigquery_client import BigQueryClient
from .config import MonitoringConfiguration
from .dryrun import dry_run_query
from .logging import LogConfiguration
from .utils import bq_normalize_name

PATH = Path(os.path.dirname(__file__))

METRIC_QUERY_FILENAME = "metric_query.sql"
METRIC_VIEW_FILENAME = "metric_view.sql"
ALERTS_FILENAME = "alerts_query.sql"
STATISTICS_QUERY_FILENAME = "statistics_query.sql"
STATISTICS_VIEW_FILENAME = "statistics_view.sql"
TEMPLATE_FOLDER = PATH / "templates"
DATA_TYPES = {"histogram", "scalar"}  # todo: enum


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
        print(f"Run metrics query for {self.slug}")
        self._run_metrics_sql(submission_date)

        print(f"Create metrics view for {self.slug}")
        self.bigquery.execute(self._get_metric_view_sql())

        print("Calculate statistics")
        self._run_statistics_sql(submission_date)

        print(f"Create statistics view for {self.slug}")
        self.bigquery.execute(self._get_statistics_view_sql())

        print(f"Create alerts data for {self.slug}")
        self._run_sql_for_alerts(submission_date)
        return True

    def _run_metrics_sql(self, submission_date: datetime):
        """Generate and execute the ETL for a specific data type."""
        try:
            self._check_runnable(submission_date)
        except Exception as e:
            print(f"Failed to run opmon project: {e}")
            return

        date_partition = str(submission_date).replace("-", "").split(" ")[0]
        destination_table = f"{self.normalized_slug}${date_partition}"

        self.bigquery.execute(
            self._get_metrics_sql(submission_date=submission_date),
            destination_table,
            clustering=["build_id"],
            time_partitioning="submission_date",
            dataset=f"{self.dataset}_derived",
        )

    def _render_sql(self, template_file: str, render_kwargs: Dict[str, Any]):
        """Render and return the SQL from a template."""
        file_loader = FileSystemLoader(TEMPLATE_FOLDER)
        env = Environment(loader=file_loader)
        template = env.get_template(template_file)
        sql = template.render(**render_kwargs)
        return sql

    def _get_metrics_sql(self, submission_date: datetime, first_run: Optional[bool] = None) -> str:
        """Return SQL for data_type ETL."""
        probes = self.config.probes

        if len(probes) == 0:
            # There are no probes for this data source + data type combo
            logger.warning(
                f"No metrics configured for {self.slug}.",
                extra={"experiment": self.slug},
            )

        # group probes that are part of the same dataset
        # necessary for creating the SQL template
        metrics_per_dataset = {}
        for probe in probes:
            if probe.metric.data_source.name not in metrics_per_dataset:
                metrics_per_dataset[probe.metric.data_source.name] = [probe.metric]
            else:
                if probe.metric not in metrics_per_dataset[probe.metric.data_source.name]:
                    metrics_per_dataset[probe.metric.data_source.name].append(probe.metric)

        # check if this is the first time the queries are executed
        # the queries are referencing the destination table if build_id is used for the time frame
        if first_run is None:
            destination_table = f"{self.project}.{self.dataset}_derived.{self.normalized_slug}"
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
            "metrics_per_dataset": metrics_per_dataset,
            "slug": self.slug,
            "normalized_slug": self.normalized_slug,
        }

        sql_filename = METRIC_QUERY_FILENAME
        sql = self._render_sql(sql_filename, render_kwargs)
        return sql

    def _get_metric_view_sql(self) -> str:
        """Return the SQL to create a BigQuery view."""
        render_kwargs = {
            "gcp_project": self.project,
            "dataset": self.dataset,
            "config": self.config.project,
            "normalized_slug": self.normalized_slug,
        }
        sql = self._render_sql(METRIC_VIEW_FILENAME, render_kwargs)
        return sql

    def _run_statistics_sql(self, submission_date):
        date_partition = str(submission_date).replace("-", "").split(" ")[0]
        destination_table = f"{self.normalized_slug}_statistics${date_partition}"

        self.bigquery.execute(
            self._get_statistics_sql(submission_date=submission_date),
            destination_table,
            clustering=["build_id"],
            time_partitioning="submission_date",
            dataset=f"{self.dataset}_derived",
        )

    def _get_statistics_sql(self, submission_date) -> str:
        """Return the SQL to run the statistics."""
        render_kwargs = {
            "gcp_project": self.project,
            "dataset": self.dataset,
            "config": self.config.project,
            "normalized_slug": self.normalized_slug,
            "dimensions": self.config.dimensions,
            "summaries": self.config.probes,
            "submission_date": submission_date,
        }
        sql = self._render_sql(STATISTICS_QUERY_FILENAME, render_kwargs)
        return sql

    def _get_statistics_view_sql(self) -> str:
        """Return the SQL to create a BigQuery view."""
        render_kwargs = {
            "gcp_project": self.project,
            "dataset": self.dataset,
            "config": self.config.project,
            "normalized_slug": self.normalized_slug,
        }
        sql = self._render_sql(STATISTICS_VIEW_FILENAME, render_kwargs)
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

    def _get_sql_for_alerts(self, submission_date) -> str:
        """Get the alerts view SQL."""
        alerts: Dict[str, Any] = {}
        for alert_type in AlertType:
            alerts[alert_type.value] = []

        for alert in self.config.alerts:
            alerts[alert.type.value].append(alert)

        render_kwargs = {
            "gcp_project": self.project,
            "dataset": self.dataset,
            "config": self.config.project,
            "normalized_slug": self.normalized_slug,
            "dimensions": self.config.dimensions,
            "alerts": alerts,
            "submission_date": submission_date,
        }

        sql = self._render_sql(ALERTS_FILENAME, render_kwargs)
        return sql

    def _run_sql_for_alerts(self, submission_date) -> None:
        try:
            self._check_runnable(submission_date)
        except Exception as e:
            print(f"Failed to run opmon project: {e}")
            return

        total_alerts = 0

        for _ in self.config.alerts:
            total_alerts += 1

        if total_alerts <= 0:
            print(f"No alerts configured for {self.normalized_slug}")
            return

        date_partition = str(submission_date).replace("-", "").split(" ")[0]
        destination_table = f"{self.normalized_slug}_alerts${date_partition}"

        self.bigquery.execute(
            self._get_sql_for_alerts(submission_date=submission_date),
            destination_table,
            clustering=["build_id"],
            time_partitioning="submission_date",
            dataset=f"{self.dataset}_derived",
        )

    def validate(self) -> None:
        """Validate ETL and configs of opmon project."""
        self._check_runnable()

        metrics_sql = self._get_metrics_sql(
            submission_date=self.config.project.start_date,  # type: ignore
            first_run=True,
        )
        dry_run_query(metrics_sql)

        dummy_probes = {}
        for summary in self.config.probes:
            if summary.metric.name not in dummy_probes:
                dummy_probes[summary.metric.name] = "1"
                if summary.metric.type == "histogram":
                    dummy_probes[
                        summary.metric.name
                    ] = """
                        STRUCT(
                            3 AS bucket_count,
                            4 AS histogram_type,
                            12 AS `sum`,
                            [1, 12] AS `range`,
                            [STRUCT(0 AS key, 12 AS value)] AS `values`
                        )
                    """

        metrics_table_dummy = f"""
            (
                SELECT
                    CURRENT_DATE() AS submission_date,
                    1 AS client_id,
                    NULL AS build_id,
                    {",".join([f"1 AS {d.name}" for d in self.config.dimensions])}
                    {"," if len(self.config.dimensions) > 0 else ""}
                    "foo" AS branch,
                    {",".join([f"{d} AS {probe}" for probe, d in dummy_probes.items()])}
            )
        """

        statistics_sql = self._get_statistics_sql(
            submission_date=self.config.project.start_date,  # type: ignore
        )
        statistics_sql = statistics_sql.replace(
            f"`{self.project}.{self.dataset}.{self.normalized_slug}`", metrics_table_dummy
        )
        statistics_sql = statistics_sql.replace(
            f"`{self.project}.{self.dataset}_derived.{self.normalized_slug}`", metrics_table_dummy
        )
        dry_run_query(statistics_sql)

        # todo: validate alerts
