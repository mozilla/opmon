"""Generate and run the Operational Monitoring Queries."""

import copy
import itertools
import os
import re
from asyncio.log import logger
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import attr
from google import cloud
from google.cloud import bigquery
from jinja2 import Environment, FileSystemLoader
from metric_config_parser.alert import AlertType
from metric_config_parser.monitoring import MonitoringConfiguration

from opmon.platform import PLATFORM_CONFIGS

from . import errors
from .bigquery_client import BeforeExecuteCallback, BigQueryClient
from .dryrun import dry_run_query
from .logging import LogConfiguration
from .statistic import Summary
from .utils import bq_normalize_name

PATH = Path(os.path.dirname(__file__))

METRIC_QUERY_FILENAME = "metric_query.sql"
METRIC_VIEW_FILENAME = "metric_view.sql"
ALERTS_QUERY_FILENAME = "alerts_query.sql"
ALERTS_VIEW_FILENAME = "alerts_view.sql"
STATISTICS_QUERY_FILENAME = "statistics_query.sql"
STATISTICS_VIEW_FILENAME = "statistics_view.sql"
TEMPLATE_FOLDER = PATH / "templates"
DATA_TYPES = {"histogram", "scalar"}  # todo: enum
SCHEMA_VERSIONS = {"metric": 1, "statistic": 2, "alert": 2}
METRICS_JOIN_KEYS = ["client_id", "submission_date", "build_id", "branch"]
MAX_DIMENSIONS_PER_METRIC_QUERY = 40
TABLE_EXPIRATION_MS = 66960000000  # expiration set to 775 days


@attr.s(auto_attribs=True)
class Monitoring:
    """Wrapper for analysing experiments."""

    project: str
    dataset: str
    derived_dataset: str
    slug: str
    config: MonitoringConfiguration
    log_config: Optional[LogConfiguration] = None
    _client: Optional[BigQueryClient] = None

    # Optional callback invoked before each BigQuery `execute`.  Parameters are
    # the BigQuery SQL string, the BigQuery job configuration (or `None`, when
    # validating), and an optional dict of consumer-provided annotations for,
    # e.g., including a relevant date, query type, etc.
    before_execute_callback: Optional[BeforeExecuteCallback] = None

    @property
    def bigquery(self):
        """Return the BigQuery client instance."""
        if not self._client:
            self._client = BigQueryClient(project=self.project, dataset=self.dataset)
            self._client.before_execute_callback = self.before_execute_callback
        return self._client

    @property
    def normalized_slug(self):
        """Return the normalized slug."""
        return bq_normalize_name(self.slug)

    def run(self, submission_date):
        """Execute and generate the operational monitoring ETL for a specific date."""
        if self.config.project.skip:
            print(f"Skipping {self.slug}")
            return True

        try:
            self._check_runnable(submission_date)
        except Exception as e:
            print(f"Failed to run opmon project: {e}")
            return

        print(f"Run metrics query for {self.slug}")
        self._run_metrics_sql(submission_date)

        print(f"Create metrics view for {self.slug}")
        self.bigquery.execute(
            self._get_metric_view_sql(),
            annotations={
                "slug": self.slug,
                "type": "metrics_view",
                "submission_date": submission_date,
            },
        )

        print(f"Calculate statistics for {self.slug}")
        self._run_statistics_sql(submission_date)

        print(f"Create statistics view for {self.slug}")
        self.bigquery.execute(
            self._get_statistics_view_sql(),
            annotations={
                "slug": self.slug,
                "type": "statistics_view",
                "submission_date": submission_date,
            },
        )

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

        table_name = f"{self.normalized_slug}_v{SCHEMA_VERSIONS['metric']}"

        join_keys = copy.deepcopy(METRICS_JOIN_KEYS)
        for dimension in self.config.dimensions:
            join_keys.append(dimension.name)

        self.bigquery.execute(
            self._get_metrics_sql(submission_date=submission_date, table_name=table_name),
            destination_table=f"{table_name}${submission_date:%Y%m%d}",
            clustering=["build_id"],
            time_partitioning="submission_date",
            partition_expiration_ms=TABLE_EXPIRATION_MS,
            write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE,
            dataset=self.derived_dataset,
            join_keys=join_keys,
            annotations={
                "slug": self.slug,
                "type": "metrics_query",
                "submission_date": submission_date,
            },
        )

    def _render_sql(self, template_file: str, render_kwargs: Dict[str, Any]):
        """Render and return the SQL from a template."""
        file_loader = FileSystemLoader(TEMPLATE_FOLDER)
        env = Environment(loader=file_loader)
        template = env.get_template(template_file)
        sql = template.render(**render_kwargs)
        return sql

    def _app_id_to_bigquery_dataset(self, app_id: Optional[str]) -> Optional[str]:
        if app_id is None:
            return None
        return re.sub(r"[^a-zA-Z0-9]", "_", app_id).lower()

    def _get_metrics_sql(
        self,
        submission_date: datetime,
        first_run: Optional[bool] = None,
        table_name: Optional[str] = None,
    ) -> Union[str, List[str]]:
        """Return SQL for data_type ETL."""
        metrics = self.config.metrics

        if len(metrics) == 0:
            # There are no metrics for this data source + data type combo
            logger.warning(
                f"No metrics configured for {self.slug}.",
                extra={"experiment": self.slug},
            )

        # group metrics that are part of the same dataset
        # necessary for creating the SQL template
        metrics_per_dataset = {}
        for metric in metrics:
            if metric.metric.data_source.name not in metrics_per_dataset:
                metrics_per_dataset[metric.metric.data_source.name] = [metric.metric]
            else:
                if metric.metric not in metrics_per_dataset[metric.metric.data_source.name]:
                    metrics_per_dataset[metric.metric.data_source.name].append(metric.metric)

        # check if this is the first time the queries are executed
        # the queries are referencing the destination table if build_id is used for the time frame
        if first_run is None:
            first_run = True
            if table_name is not None:
                try:
                    self.bigquery.client.get_table(
                        f"{self.project}.{self.derived_dataset}.{table_name}"
                    )
                except cloud.exceptions.NotFound:
                    first_run = True
                else:
                    first_run = False

        print(first_run)

        render_kwargs = {
            "header": "-- Generated via opmon\n",
            "gcp_project": self.project,
            "submission_date": submission_date,
            "config": self.config.project,
            "dataset": self.dataset,
            "first_run": first_run,
            "slug": self.slug,
            "normalized_slug": self.normalized_slug,
            "table_version": SCHEMA_VERSIONS["metric"],
            "is_glean_app": PLATFORM_CONFIGS[
                (
                    self.config.project.app_name or "firefox_desktop"
                    if self.config.project
                    else "firefox_desktop"
                )
            ].is_glean_app,
            "app_id": self._app_id_to_bigquery_dataset(
                PLATFORM_CONFIGS[
                    (
                        self.config.project.app_name or "firefox_desktop"
                        if self.config.project
                        else "firefox_desktop"
                    )
                ].app_id.get(
                    # mypy doesn't recognize the enum value as str, so ignore
                    (
                        self.config.project.population.channel.value  # type: ignore
                        if self.config.project.population.channel
                        else None
                    ),
                    None,
                )
            ),
            "dimensions": self.config.dimensions,
        }

        sql_filename = METRIC_QUERY_FILENAME

        # chunk queries and render multiple times
        i = MAX_DIMENSIONS_PER_METRIC_QUERY
        metrics_chunk: Dict[str, Any] = {}
        sql = []
        for data_source, metrics in metrics_per_dataset.items():
            for metric in metrics:
                if i <= 0:
                    sql.append(
                        self._render_sql(
                            sql_filename,
                            {"metrics_per_dataset": metrics_chunk, **render_kwargs},
                        )
                    )
                    i = MAX_DIMENSIONS_PER_METRIC_QUERY
                    metrics_chunk = {}

                if data_source not in metrics_chunk:
                    metrics_chunk[data_source] = [metric]
                else:
                    metrics_chunk[data_source].append(metric)
                i -= 1

        sql.append(
            self._render_sql(sql_filename, {"metrics_per_dataset": metrics_chunk, **render_kwargs})
        )

        if len(sql) == 1:
            return sql[0]
        return sql

    def _get_metric_view_sql(self) -> str:
        """Return the SQL to create a BigQuery view."""
        render_kwargs = {
            "gcp_project": self.project,
            "derived_dataset": self.derived_dataset,
            "dataset": self.dataset,
            "config": self.config.project,
            "normalized_slug": self.normalized_slug,
            "table_version": SCHEMA_VERSIONS["metric"],
        }
        sql = self._render_sql(METRIC_VIEW_FILENAME, render_kwargs)
        return sql

    def _run_statistics_sql(self, submission_date):
        table_name = f"{self.normalized_slug}_statistics_v{SCHEMA_VERSIONS['statistic']}"
        self.bigquery.execute(
            self._get_statistics_sql(submission_date=submission_date),
            destination_table=f"{table_name}${submission_date:%Y%m%d}",
            clustering=["build_id"],
            time_partitioning="submission_date",
            write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE,
            dataset=self.derived_dataset,
        )

    def _get_statistics_sql(self, submission_date) -> str:
        """Return the SQL to run the statistics."""
        render_kwargs = {
            "gcp_project": self.project,
            "dataset": self.dataset,
            "derived_dataset": self.derived_dataset,
            "config": self.config.project,
            "normalized_slug": self.normalized_slug,
            "dimensions": self.config.dimensions,
            "dimension_permutations": [
                list(i)
                for i in itertools.product([True, False], repeat=len(self.config.dimensions))
                if any(i)
            ],
            "summaries": [Summary.from_config(summary) for summary in self.config.metrics],
            "submission_date": submission_date,
            "table_version": SCHEMA_VERSIONS["metric"],
        }
        sql = self._render_sql(STATISTICS_QUERY_FILENAME, render_kwargs)
        return sql

    def _get_statistics_view_sql(self) -> str:
        """Return the SQL to create a BigQuery view."""
        render_kwargs = {
            "gcp_project": self.project,
            "dataset": self.dataset,
            "derived_dataset": self.derived_dataset,
            "config": self.config.project,
            "normalized_slug": self.normalized_slug,
            "table_version": SCHEMA_VERSIONS["statistic"],
            "summaries": [Summary.from_config(summary) for summary in self.config.metrics],
            "dimensions": self.config.dimensions,
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

        sql = self._render_sql(ALERTS_QUERY_FILENAME, render_kwargs)
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

        table_name = f"{self.normalized_slug}_alerts_v{SCHEMA_VERSIONS['alert']}"
        self.bigquery.execute(
            self._get_sql_for_alerts(submission_date=submission_date),
            destination_table=f"{table_name}${submission_date:%Y%m%d}",
            clustering=["build_id"],
            time_partitioning="submission_date",
            write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE,
            dataset=self.derived_dataset,
        )

        print(f"Create alerts view for {self.slug}")
        self.bigquery.execute(
            self._get_alerts_view_sql(),
            annotations={
                "slug": self.slug,
                "type": "alerts_view",
                "submission_date": submission_date,
            },
        )

    def _get_alerts_view_sql(self) -> str:
        """Return the SQL to create a BigQuery view."""
        render_kwargs = {
            "gcp_project": self.project,
            "dataset": self.dataset,
            "derived_dataset": self.derived_dataset,
            "normalized_slug": self.normalized_slug,
            "table_version": SCHEMA_VERSIONS["alert"],
        }
        sql = self._render_sql(ALERTS_VIEW_FILENAME, render_kwargs)
        return sql

    def validate(self) -> None:
        """Validate ETL and configs of opmon project."""
        self._check_runnable()

        if self.config.project and self.config.project.skip:
            return

        metrics_sql = self._get_metrics_sql(
            submission_date=self.config.project.start_date,  # type: ignore
            first_run=True,
        )
        print(f"Dry run metrics SQL for {self.normalized_slug}")

        if callable(self.before_execute_callback):
            # Before and after are the same query for metrics: there's no
            # modification preparing for dry run.
            if isinstance(metrics_sql, list):
                for idx, query in enumerate(metrics_sql):
                    self.before_execute_callback(
                        query,
                        None,
                        annotations={
                            "slug": self.slug,
                            "type": "validate_metrics_query_before",
                            "submission_date": self.config.project.start_date,
                            "part": f"part-{idx}",
                        },
                    )
                self.before_execute_callback(
                    query,
                    None,
                    annotations={
                        "slug": self.slug,
                        "type": "validate_metrics_query_after",
                        "submission_date": self.config.project.start_date,
                        "part": f"part-{idx}",
                    },
                )
            else:
                self.before_execute_callback(
                    metrics_sql,
                    None,
                    annotations={
                        "slug": self.slug,
                        "type": "validate_metrics_query_before",
                        "submission_date": self.config.project.start_date,
                    },
                )
                self.before_execute_callback(
                    metrics_sql,
                    None,
                    annotations={
                        "slug": self.slug,
                        "type": "validate_metrics_query_after",
                        "submission_date": self.config.project.start_date,
                    },
                )
        dry_run_query(metrics_sql)

        dummy_metrics = {}
        for summary in self.config.metrics:
            if summary.metric.name not in dummy_metrics:
                dummy_metrics[summary.metric.name] = "1"
                if summary.metric.type == "histogram":
                    dummy_metrics[
                        summary.metric.name
                    ] = """
                        [STRUCT(
                            3 AS bucket_count,
                            4 AS histogram_type,
                            12 AS `sum`,
                            [1, 12] AS `range`,
                            [STRUCT(0 AS key, 12 AS value)] AS `values`
                        )]
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
                    {",".join([f"{d} AS {metric}" for metric, d in dummy_metrics.items()])}
            )
        """

        statistics_sql = self._get_statistics_sql(
            submission_date=self.config.project.start_date,  # type: ignore
        )

        # The original query is more useful for inspection.
        if callable(self.before_execute_callback):
            self.before_execute_callback(
                statistics_sql,
                None,
                annotations={
                    "slug": self.slug,
                    "type": "validate_statistics_query_before",
                    "submission_date": self.config.project.start_date,
                },
            )

        statistics_sql = statistics_sql.replace(
            f"`{self.project}.{self.dataset}.{self.normalized_slug}`",
            metrics_table_dummy,
        )
        statistics_sql = statistics_sql.replace(
            f"`{self.project}.{self.derived_dataset}.{self.normalized_slug}"
            + f"_v{SCHEMA_VERSIONS['metric']}`",
            metrics_table_dummy,
        )
        print(f"Dry run statistics SQL for {self.normalized_slug}")

        # But the modified query is what is actually submitted.
        if callable(self.before_execute_callback):
            self.before_execute_callback(
                statistics_sql,
                None,
                annotations={
                    "slug": self.slug,
                    "type": "validate_statistics_query_after",
                    "submission_date": self.config.project.start_date,
                },
            )
        dry_run_query(statistics_sql)

        total_alerts = 0
        for _ in self.config.alerts:
            total_alerts += 1

        if total_alerts > 0:
            statistics_table_dummy = f"""
                (
                    SELECT
                        CURRENT_DATE() AS submission_date,
                        NULL AS build_id,
                        "test" AS metric,
                        "test" AS statistic,
                        "disabled" AS branch,
                        {",".join([f"1 AS {d.name}" for d in self.config.dimensions])}
                        {"," if len(self.config.dimensions) > 0 else ""}
                        1.2 AS point,
                        NULL AS lower,
                        NULL AS upper,
                        NULL AS parameter
                )
            """
            alerts_sql = self._get_sql_for_alerts(
                submission_date=self.config.project.start_date,  # type: ignore
            )

            # Again, the original is more useful for inspection.
            if callable(self.before_execute_callback):
                self.before_execute_callback(
                    alerts_sql,
                    None,
                    annotations={
                        "slug": self.slug,
                        "type": "validate_alerts_query_before",
                        "submission_date": self.config.project.start_date,
                    },
                )

            alerts_sql = alerts_sql.replace(
                f"`{self.project}.{self.dataset}.{self.normalized_slug}_statistics`",
                statistics_table_dummy,
            )
            print(f"Dry run alerts SQL for {self.normalized_slug}")

            if callable(self.before_execute_callback):
                self.before_execute_callback(
                    alerts_sql,
                    None,
                    annotations={
                        "slug": self.slug,
                        "type": "validate_alerts_query_after",
                        "submission_date": self.config.project.start_date,
                    },
                )
            dry_run_query(alerts_sql)
