"""Converts the old JSON config file format to toml."""

import toml
import json
import click

@click.command()
@click.argument(
    "path",
)
def convert(path):
    with open(path) as f:
        config = json.load(f)
    
    converted_conf = {
        "probes": {}
    }

    for name, c in config.items():
        converted_conf["probes"][name.lower()] = {
            "data_source": c["source"].split(".")[-1],
            "select_expression": c["sql"],
            "friendly_name": name.replace("_", " ").title(),
            "category": c["category"],
            "type": c["type"]
        }

    print(toml.dumps(converted_conf))

if __name__ == '__main__':
    convert()
