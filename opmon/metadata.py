"""Metadata handler for opmon projects."""

import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

import attr
from jinja2 import Environment, FileSystemLoader

from opmon.bigquery_client import BigQueryClient
from opmon.config import MonitoringConfiguration

PATH = Path(os.path.dirname(__file__))
PROJECTS_TABLE = "projects_v1"
PROJECTS_FILENAME = "projects.sql"
TEMPLATE_FOLDER = PATH / "templates"


@attr.s(auto_attribs=True)
class Metadata:
    """Handler for writing metadata for opmon projects."""

    project: str
    dataset: str
    projects: List[Tuple[str, MonitoringConfiguration]]

    @property
    def bigquery(self):
        """Return the BigQuery client instance."""
        return BigQueryClient(project=self.project, dataset=self.dataset)

    def _render_sql(self, template_file: str, render_kwargs: Dict[str, Any]):
        """Render and return the SQL from a template."""
        file_loader = FileSystemLoader(TEMPLATE_FOLDER)
        env = Environment(loader=file_loader)
        template = env.get_template(template_file)
        sql = template.render(**render_kwargs)
        return sql

    def write(self) -> None:
        """Update the BQ table with project metadata."""
        destination_table = f"{self.project}.{self.dataset}_derived.{PROJECTS_TABLE}"

        # check if projects metadata table exists; otherwise it needs to be created
        first_run = True
        try:
            self.bigquery.client.get_table(destination_table)
            first_run = False
        except Exception:
            first_run = True

        project_metadata: List[Dict[str, Any]] = []

        for slug, config in self.projects:
            probes = config.probes
            render_probes = [
                {"name": probe.name, "agg_type": probe.type} for probe in probes if probe.type
            ]

            project_metadata.append(
                {
                    "slug": slug,
                    "dimensions": config.dimensions,
                    "probes": render_probes,
                    "config": config.project,
                }
            )

        render_kwargs = {
            "gcp_project": self.project,
            "dataset": self.dataset,
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
                FROM `{self.project}.{self.dataset}_derived.{PROJECTS_TABLE}`
            )
        """

        self.bigquery.execute(view_query)
        print("Updated project metadata")
