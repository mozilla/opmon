"""Metadata handler for opmon projects."""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import attr
from jinja2 import Environment, FileSystemLoader
from metric_config_parser.monitoring import MonitoringConfiguration

from opmon.bigquery_client import BigQueryClient
from opmon.statistic import Summary

PATH = Path(os.path.dirname(__file__))
PROJECTS_TABLE = "projects_v1"
PROJECTS_FILENAME = "projects.sql"
TEMPLATE_FOLDER = PATH / "templates"


@attr.s(auto_attribs=True)
class Metadata:
    """Handler for writing metadata for opmon projects."""

    project: str
    dataset: str
    derived_dataset: str
    projects: List[Tuple[str, MonitoringConfiguration]]
    _client: Optional[BigQueryClient] = None

    @property
    def bigquery(self):
        """Return the BigQuery client instance."""
        self._client = self._client or BigQueryClient(project=self.project, dataset=self.dataset)
        return self._client

    def _render_sql(self, template_file: str, render_kwargs: Dict[str, Any]):
        """Render and return the SQL from a template."""
        file_loader = FileSystemLoader(TEMPLATE_FOLDER)
        env = Environment(loader=file_loader)
        template = env.get_template(template_file)
        sql = template.render(**render_kwargs)
        return sql

    def write(self) -> None:
        """Update the BQ table with project metadata."""
        destination_table = f"{self.project}.{self.derived_dataset}.{PROJECTS_TABLE}"

        # check if projects metadata table exists; otherwise it needs to be created
        first_run = True
        try:
            self.bigquery.client.get_table(destination_table)
            first_run = False
        except Exception:
            first_run = True

        project_metadata: List[Dict[str, Any]] = []

        for slug, config in self.projects:
            summaries = config.metrics
            render_summaries = [
                {
                    "metric": summary.metric.name,
                    "statistic": Summary.from_config(summary).statistic.name(),
                }
                for summary in summaries
            ]

            if (
                config.project
                and config.project.end_date
                and config.project.start_date
                and config.project.end_date <= config.project.start_date
            ):
                continue

            metric_groups: Dict[str, List[Any]] = {}
            for metric_group in config.project.metric_groups:
                for metric in metric_group.metrics:
                    if metric.name in metric_groups:
                        metric_groups[metric.name].append(metric_group.name)
                    else:
                        metric_groups[metric.name] = [metric_group.name]

            project_metadata.append(
                {
                    "slug": slug,
                    "dimensions": config.dimensions,
                    "summaries": render_summaries,
                    "config": config.project,
                    "alerts": config.alerts,
                    "metric_groups": metric_groups,
                }
            )

        render_kwargs = {
            "gcp_project": self.project,
            "dataset": self.dataset,
            "derived_dataset": self.derived_dataset,
            "table": PROJECTS_TABLE,
            "projects": project_metadata,
            "first_run": first_run,
        }
        query = self._render_sql(PROJECTS_FILENAME, render_kwargs=render_kwargs)
        self.bigquery.execute(query)

        # Create view
        view_name = PROJECTS_TABLE.split("_")[0]
        view_query = f"""
            CREATE OR REPLACE VIEW `{self.project}.{self.dataset}.{view_name}` AS (
                SELECT *
                FROM `{self.project}.{self.derived_dataset}.{PROJECTS_TABLE}`
            )
        """

        self.bigquery.execute(view_query)
        print("Updated project metadata")
