import time
import math
from concurrent import futures
from loguru import logger
from typing import Any

import tweepy

from tweepipe.utils import tracker


def run_parallel(fn: Any, kwargs_list: list, max_workers: int = None) -> list:
    """Parallelize a function call.

    :param fn: Function to be run in parallel.
    :type fn: function
    :param kwargs_list: List of keyword arguments to be provided with
        function call.
    :type kwargs_list: list
    :param max_workers: Maximum number of process employed to run function,
        defaults to None.
    :type max_workers: int, optional
    :return: Results returned by individual function calls.
    :rtype: list
    """

    executor_kwargs = dict(max_workers=max_workers)
    as_completed_fn = futures.as_completed

    with futures.ProcessPoolExecutor(**executor_kwargs) as executor:
        # log start of streaming/searching process
        for kwargs in kwargs_list:
            if "keywords" in kwargs:
                logger.info(
                    f'Running streaming for {kwargs["issue"]} with keywords: {kwargs["keywords"]}.'
                )

        # initial submission of the streaming tasks
        futures_list = {executor.submit(fn, **kwargs): kwargs for kwargs in kwargs_list}

        results = []
        for i, future in enumerate(as_completed_fn(futures_list)):
            kwargs = futures_list[future]
            try:
                result = future.result()
            except Exception as e:
                error_msg = f"Error while running parallel task with args {kwargs}."
                tracker.log_error(e, msg=error_msg)
                logger.error(error_msg)

            results.append(result)
            logger.info(f"Execution done for ({i+1}/{len(kwargs_list)})")

    return results


def get_lookup_users_kwargs_list(
    uids: list = [], apis: list = [], issue: str = None, env_file: str = None
):
    uid_batch_size = math.ceil(len(uids) / len(apis))
    kwargs_list = []

    for i, api in zip(list(range(0, len(uids), uid_batch_size)), apis):
        kwargs_list.append(
            {
                "uids": uids[i : i + uid_batch_size],
                "twitter_api": api,
                "issue": issue,
                "env_file": env_file,
            }
        )

    return kwargs_list
