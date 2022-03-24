import logging
import os
import sys
from datetime import datetime, timedelta
from functools import partial
from multiprocessing.pool import ThreadPool
from pathlib import Path
from typing import Iterable, Tuple

import click
import pytz

from opmon.config import MonitoringConfiguration
from opmon.dryrun import DryRunFailedError
from opmon.experimenter import ExperimentCollection
from opmon.external_config import ExternalConfigCollection, entity_from_path
from opmon.logging import LogConfiguration
from opmon.monitoring import Monitoring

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
    required=False,
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
    default="moz-fx-data-shared-prod",
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
    help="Date for which projects should be analyzed",
    metavar="YYYY-MM-DD",
    required=True,
)
@slug_option
@parallelism_option
def run(project_id, dataset_id, date, slug, parallelism):
    external_configs = ExternalConfigCollection.from_github_repo()
    platform_definitions = external_configs.definitions
    experiments = ExperimentCollection.from_experimenter().ever_launched()

    # get and resolve configs for projects
    configs = []
    for external_config in external_configs.configs:
        if slug:
            if external_config.slug != slug:
                continue

        experiment = experiments.with_slug(external_config.slug)
        platform = external_config.spec.project.platform or experiment.app_name or DEFAULT_PLATFORM

        if platform not in external_configs.definitions:
            logger.exception(
                str(f"Invalid platform {platform}"),
                exc_info=None,
                extra={"experiment": experiment.normandy_slug},
            )
            continue

        platform_definitions = external_configs.definitions[platform]
        spec = platform_definitions.spec
        spec.merge(external_config.spec)
        configs.append((external_config.slug, spec.resolve(experiment)))

    # filter out projects that have finished or not started
    prior_date = date - timedelta(days=1)
    configs = [
        (k, cfg)
        for (k, cfg) in configs
        if cfg.project.start_date <= prior_date and cfg.project.end_date >= prior_date
    ]

    run = partial(_run, project_id, dataset_id, prior_date)

    with ThreadPool(parallelism) as pool:
        pool.map(run, configs)

    success = True  # todo
    sys.exit(0 if success else 1)


def _run(
    project_id: str,
    dataset_id: str,
    submission_date: datetime,
    config: Tuple[str, MonitoringConfiguration],
):
    monitoring = Monitoring(
        project=project_id, dataset=dataset_id, slug=config[0], config=config[1]
    )
    monitoring.run(submission_date)
    return True


@cli.command()
@project_id_option
@dataset_id_option
@click.option(
    "--start_date",
    "--start-date",
    type=ClickDate(),
    help="Date for which project should be started to get analyzed",
    metavar="YYYY-MM-DD",
    required=True,
)
@click.option(
    "--end_date",
    "--end-date",
    type=ClickDate(),
    help="Date for which project should be stop to get analyzed",
    metavar="YYYY-MM-DD",
    required=True,
)
@click.option(
    "--slug",
    help="Experimenter or Normandy slug associated with the project to backfill the analysis for",
    required=True,
)
def backfill(project_id, dataset_id, start_date, end_date, slug):
    """Backfill project."""
    external_configs = ExternalConfigCollection.from_github_repo()
    platform_definitions = external_configs.definitions
    experiments = ExperimentCollection.from_experimenter().ever_launched()

    # get and resolve configs for projects
    config = None
    for external_config in external_configs.configs:
        if external_config.slug != slug:
            continue

        experiment = experiments.with_slug(external_config.slug)
        platform = external_config.spec.project.platform or experiment.app_name or DEFAULT_PLATFORM

        if platform not in external_configs.definitions:
            logger.exception(
                str(f"Invalid platform {platform}"),
                exc_info=None,
                extra={"experiment": experiment.normandy_slug},
            )
            continue

        platform_definitions = external_configs.definitions[platform]
        spec = platform_definitions.spec
        spec.merge(external_config.spec)
        config = (external_config.slug, spec.resolve(experiment))
        break

    # determine backfill time frame based on start and end dates
    start_date = (
        start_date
        if config[1].project.start_date is None
        else max(config[1].project.start_date, start_date)
    )
    end_date = (
        end_date
        if config[1].project.end_date is None
        else min(config[1].project.end_date, end_date)
    )

    success = True

    print(f"Start running backfill for {config[0]}: {start_date} to {end_date}")
    # backfill needs to be run sequentially since data is required from previous runs
    for date in [
        start_date + timedelta(days=d) for d in range(0, (end_date - start_date).days + 1)
    ]:
        print(f"Backfill {date}")
        try:
            monitoring = Monitoring(
                project=project_id, dataset=dataset_id, slug=config[0], config=config[1]
            )
            monitoring.run(date)
        except Exception as e:
            print(f"Error backfilling {config[0]}: {e}")
            success = False

    sys.exit(0 if success else 1)


@cli.command("validate_config")
@click.argument("path", type=click.Path(exists=True), nargs=-1)
def validate_config(path: Iterable[os.PathLike]):
    """Validate config files."""
    dirty = False
    external_configs = ExternalConfigCollection.from_github_repo()
    platform_definitions = external_configs.definitions
    experiments = ExperimentCollection.from_experimenter().ever_launched()

    for config_file in path:
        config_file = Path(config_file)
        if not config_file.is_file():
            continue
        if ".example" in config_file.suffixes:
            print(f"Skipping example config {config_file}")
            continue
        print(f"Evaluating {config_file}...")
        entity = entity_from_path(config_file)
        experiment = experiments.with_slug(entity.slug)
        platform = entity.spec.project.platform or experiment.app_name or DEFAULT_PLATFORM

        if platform not in platform_definitions:
            print(f"Invalid platform {platform}")
            dirty = True
            continue

        platform_definitions = external_configs.definitions[platform]
        spec = entity.spec
        spec.merge(platform_definitions.spec)

        try:
            entity.validate(experiment)
        except DryRunFailedError as e:
            print("Error evaluating SQL:")
            for i, line in enumerate(e.sql.split("\n")):
                print(f"{i+1: 4d} {line.rstrip()}")
            print("")
            print(str(e))
            dirty = True
    sys.exit(1 if dirty else 0)
