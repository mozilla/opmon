import logging
import re
import shutil
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Optional

from requests import Session

logger = logging.getLogger(__name__)


@contextmanager
def TemporaryDirectory():
    name = Path(tempfile.mkdtemp())
    try:
        yield name
    finally:
        shutil.rmtree(name)


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
