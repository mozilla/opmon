import re

from jinja2 import Environment, FileSystemLoader
import logging
import time

from typing import Any, Optional

from requests import Session

logger = logging.getLogger(__name__)


def write_sql(output_dir, full_table_id, basename, sql):
    """Write out a query to a location based on the table ID.

    :param output_dir:    Base target directory (probably sql/moz-fx-data-shared-prod/)
    :param full_table_id: Table ID in project.dataset.table form
    :param basename:      The name to give the written file (like query.sql)
    :param sql:           The query content to write out
    """
    d = get_table_dir(output_dir, full_table_id)
    d.mkdir(parents=True, exist_ok=True)
    target = d / basename
    logging.info(f"Writing {target}")
    with target.open("w") as f:
        f.write(sql)
        f.write("\n")


def render(sql_filename, format=True, template_folder="glean_usage", **kwargs) -> str:
    """Render a given template query using Jinja."""
    file_loader = FileSystemLoader(f"{template_folder}/templates")
    env = Environment(loader=file_loader)
    main_sql = env.get_template(sql_filename)
    rendered = main_sql.render(**kwargs)
    if format:
        rendered = reformat(rendered)
    return rendered


def bq_normalize_name(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", name)


class RetryLimitExceededException(Exception):
    pass


def retry_get(
    session: Session, url: str, max_retries: int, user_agent: Optional[str] = None
) -> Any:
    """
    Call an API and automatically retry if there was an error.

    This is handy for working with the Experimenter API which occassionally
    experiences some issues and returns a failure code.
    """
    # based on https://stackoverflow.com/a/22726782
    for _i in range(max_retries):
        try:
            if user_agent:
                session.headers.update({"user-agent": user_agent})

            blob = session.get(url).json()
            break
        except Exception as e:
            print(e)
            logger.info(f"Error fetching from {url}. Retrying...")
            time.sleep(1)
    else:
        exception = RetryLimitExceededException(f"Too many retries for {url}")

        logger.exception(exception.__str__(), exc_info=exception)
        raise exception

    return blob
