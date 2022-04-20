"""OpMon CLI."""
import copy
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
from opmon.external_config import (
    DEFAULTS_DIR,
    ExternalConfigCollection,
    entity_from_path,
)
from opmon.logging import LogConfiguration
from opmon.monitoring import Monitoring

logger = logging.getLogger(__name__)


DEFAULT_PLATFORM = "firefox_desktop"
DEFINITIONS_DIR = "definitions"


class ClickDate(click.ParamType):
    """Converter for click date string parameters to datetime."""

    name = "date"

    def convert(self, value, param, ctx):
        """Convert a string to datetime."""
        if isinstance(value, datetime):
            return value
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=pytz.utc)


project_id_option = click.option(
    "--project_id",
    "--project-id",
    default="moz-fx-data-shared-prod",
    help="Project to write to",
)
dataset_id_option = click.option(
    "--dataset_id",
    "--dataset-id",
    default="operational_monitoring",
    help="Publicly accessible dataset to write to. "
    + "Tables will get written to corresponding _derived dataset",
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
    default="operational_monitoring_derived",
    help="Dataset to write logs to",
)
@click.option(
    "--log_table_id", "--log-table-id", default="opmon_logs_v1", help="Table to write logs to"
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
    """Initialize CLI."""
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
    """Execute the monitoring ETL for a specific date."""
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

        # resolve config by applying platform and custom config specs
        platform_definitions = external_configs.definitions[platform]
        spec = copy.deepcopy(platform_definitions.spec)
        spec.merge(external_configs.default_spec_for_platform(platform))

        if experiment and experiment.is_rollout:
            spec.merge(external_configs.default_spec_for_type("rollout"))

        configs.append((external_config.slug, spec.resolve(experiment)))

    # prepare rollouts that do not have an external config
    rollouts = experiments.rollouts().experiments
    for rollout in rollouts:
        if not any([c[0] == rollout.normandy_slug for c in configs]):
            platform = rollout.app_name or DEFAULT_PLATFORM

            if platform not in external_configs.definitions:
                logger.exception(
                    str(f"Invalid platform {platform}"),
                    exc_info=None,
                    extra={"experiment": rollout.normandy_slug},
                )
                continue

            # resolve config by applying platform and custom config specs
            platform_definitions = external_configs.definitions[platform]
            spec = copy.deepcopy(platform_definitions.spec)
            spec.merge(external_configs.default_spec_for_platform(platform))
            spec.merge(external_configs.default_spec_for_type("rollout"))
            configs.append((rollout.normandy_slug, spec.resolve(rollout)))

    # filter out projects that have finished or not started
    prior_date = date - timedelta(days=1)
    configs = [
        (k, cfg)
        for (k, cfg) in configs
        if (cfg.project.start_date and cfg.project.start_date <= prior_date)
        and (cfg.project.end_date is None or cfg.project.end_date >= prior_date)
    ]

    run = partial(_run, project_id, dataset_id, prior_date)

    success = False
    with ThreadPool(parallelism) as pool:
        results = pool.map(run, configs)
        success = all(results)

    sys.exit(0 if success else 1)


def _run(
    project_id: str,
    dataset_id: str,
    submission_date: datetime,
    config: Tuple[str, MonitoringConfiguration],
):
    """Execute by parallel processes."""
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
    """Backfill a specific project."""
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
        spec.merge(external_configs.default_spec_for_platform(platform))
        spec.merge(external_config.spec)
        config = (external_config.slug, spec.resolve(experiment))
        break

    # check if backfill is for a rollout
    if config is None:
        rollouts = experiments.rollouts().experiments
        for rollout in rollouts:
            if rollout.normandy_slug == slug:
                platform = rollout.app_name or DEFAULT_PLATFORM

                if platform not in external_configs.definitions:
                    logger.exception(
                        str(f"Invalid platform {platform}"),
                        exc_info=None,
                        extra={"experiment": rollout.normandy_slug},
                    )
                    continue

                # resolve config by applying platform and custom config specs
                platform_definitions = external_configs.definitions[platform]
                spec = copy.deepcopy(platform_definitions.spec)
                spec.merge(external_configs.default_spec_for_platform(platform))
                spec.merge(external_configs.default_spec_for_type("rollout"))
                config = (rollout.normandy_slug, spec.resolve(rollout))
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

    # get updated definition files
    updated_definitions = {}
    for config_file in path:
        config_file = Path(config_file)
        if not config_file.is_file():
            continue
        if ".example" in config_file.suffixes:
            print(f"Skipping example config {config_file}")
            continue

        if config_file.parent.name == DEFINITIONS_DIR:
            updated_definitions[config_file.stem] = entity_from_path(config_file)

    platform_definitions.update(updated_definitions)

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
        monitor_entire_population = False
        if entity.spec.project and entity.spec.project.population:
            monitor_entire_population = entity.spec.project.population.monitor_entire_population

        if config_file.parent.name != DEFINITIONS_DIR and config_file.parent.name != DEFAULTS_DIR:
            if experiment is None and monitor_entire_population is False:
                print(f"No experiment with slug {entity.slug} in Experimenter.")
                dirty = True
                break
        else:
            # set dummy date for validating defaults
            entity.spec.project.start_date = "2022-01-01"

        if config_file.parent.name != DEFINITIONS_DIR:
            platform = (
                entity.spec.project.platform
                or (experiment.app_name if experiment else None)
                or DEFAULT_PLATFORM
            )

            if platform not in platform_definitions:
                print(f"Invalid platform {platform}")
                dirty = True
                continue

            platform_definition = platform_definitions[platform]
            spec = entity.spec
            spec.merge(platform_definition.spec)

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
