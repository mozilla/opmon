import logging
import os
import sys
from datetime import datetime, timedelta
from functools import partial
from multiprocessing.pool import Pool
from pathlib import Path
from typing import Iterable, Tuple

import click
import pytz

from opmon.config import MonitoringConfiguration
from opmon.experimenter import ExperimentCollection
from opmon.external_config import ExternalConfigCollection
from opmon.monitoring import Monitoring

from .logging import LogConfiguration

logger = logging.getLogger(__name__)


DEFAULT_PLATFORM = "firefox_desktop"


class ClickDate(click.ParamType):
    name = "date"

    def convert(self, value, param, ctx):
        if isinstance(value, datetime):
            return value
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=pytz.utc)


project_id_option = click.option(
    "--project_id",
    "--project-id",
    default="moz-fx-shared-prod",
    help="Project to write to",
)
dataset_id_option = click.option(
    "--dataset_id",
    "--dataset-id",
    default="operational_monitoring",
    help="Publicly accessible dataset to write to. "
    + "Tables will get written to corresponding _derived dataset",
    required=True,
)

slug_option = click.option(
    "--slug",
    help="Experimenter or Normandy slug associated with the project to (re)run the analysis for",
)

config_file_option = click.option(
    "--config_file", "--config-file", type=click.File("rt"), hidden=True
)

parallelism_option = click.option(
    "--parallelism", "-p", help="Number of processes to run monitoring analysis", default=8
)


@click.group()
@click.option(
    "--log_project_id",
    "--log-project-id",
    default="moz-fx-data-experiments",
    help="GCP project to write logs to",
)
@click.option(
    "--log_dataset_id",
    "--log-dataset-id",
    default="monitoring",
    help="Dataset to write logs to",
)
@click.option(
    "--log_table_id", "--log-table-id", default="opmon_logs", help="Table to write logs to"
)
@click.option("--log_to_bigquery", "--log-to-bigquery", is_flag=True, default=False)
@click.pass_context
def cli(
    ctx,
    log_project_id,
    log_dataset_id,
    log_table_id,
    log_to_bigquery,
):
    log_config = LogConfiguration(
        log_project_id,
        log_dataset_id,
        log_table_id,
        log_to_bigquery,
    )
    log_config.setup_logger()
    ctx.ensure_object(dict)
    ctx.obj["log_config"] = log_config


@cli.command()
@project_id_option
@dataset_id_option
@click.option(
    "--date",
    type=ClickDate(),
    help="Date for which experiments should be analyzed",
    metavar="YYYY-MM-DD",
    required=True,
)
@slug_option
@parallelism_option
@click.pass_context
def run(ctx, project_id, dataset_id, date, slug, parallelism):
    external_configs = ExternalConfigCollection.from_github_repo()
    platform_definitions = external_configs.definitions
    experiments = ExperimentCollection.from_experimenter().ever_launched()

    # get and resolve configs for projects
    configs = []
    for external_config in external_configs.configs:
        experiment = experiments.with_slug(external_config.slug)
        platform = external_config.spec.platform or experiment.app_name or DEFAULT_PLATFORM

        if platform not in external_config.definitions:
            logger.exception(
                str(f"Invalid platform {platform}"),
                exc_info=None,
                extra={"experiment": experiment.normandy_slug},
            )
            continue

        platform_definitions = external_config.definitions[platform]
        spec = external_config.spec
        spec.merge(platform_definitions)
        configs.append((external_config.slug, spec.resolve(experiment)))

    # filter out projects that have finished or not started
    prior_date = date - timedelta(days=1)
    configs = {
        (k, cfg)
        for (k, cfg) in configs
        if cfg.start_date <= prior_date and cfg.end_date >= prior_date
    }

    def _run(
        project_id: str,
        dataset_id: str,
        submission_date: datetime,
        config: Tuple[str, MonitoringConfiguration],
    ):
        monitoring = Monitoring(
            project_id=project_id, dataset_id=dataset_id, slug=config[0], config=config[1]
        )
        monitoring.run(submission_date)
        return True

    run = partial(_run, project_id, dataset_id, date)

    with Pool(parallelism) as pool:
        pool.map(run, configs)

    success = True
    sys.exit(0 if success else 1)


@cli.command("validate_config")
@click.argument("path", type=click.Path(exists=True), nargs=-1)
def validate_config(path: Iterable[os.PathLike]):
    """Validate config files."""
    dirty = False
    # collection = ExperimentCollection.from_experimenter()

    for config_file in path:
        config_file = Path(config_file)
        if not config_file.is_file():
            continue
        if ".example" in config_file.suffixes:
            print(f"Skipping example config {config_file}")
            continue
        print(f"Evaluating {config_file}...")
        # todo: run validation
    sys.exit(1 if dirty else 0)
