from metric_config_parser.monitoring import MonitoringConfiguration, MonitoringSpec
import toml

from opmon.config import ConfigLoader
from opmon.monitoring import Monitoring


spec = MonitoringSpec.from_dict(toml.load("mozilla_vpn.toml")).resolve(experiment=None, configs=ConfigLoader.configs)
config = ("desktop-dau", spec)
monitoring = Monitoring(
        project="moz-fx-data-shared-prod",
        dataset="some_dataset",
        derived_dataset="some_derived_dataset",
        slug=config[0],
        config=config[1],
        before_execute_callback=None,
    )
sql = monitoring._get_metrics_sql(first_run=True, submission_date="2024-02-09", table_name="my-table")
print(sql)