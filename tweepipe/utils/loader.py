import json
import pickle
import math

import jsonlines
from loguru import logger

from tweepipe import settings
from tweepipe.db import db_client
from tweepipe.utils import errors


def _save_to_json(content=None, output_file=None, mode="w"):
    """Simple wrapper to write content to a given json file."""

    with open(output_file, mode) as f:
        json.dump(content, f)


def _save_to_jsonl(content: list = [], output_file: str = None, mode: str = "w"):
    """Wrapper to write lists to a jsonl file (successive lines)."""

    with open(output_file, mode) as f:
        with jsonlines.Writer(f) as writer:
            writer.write_all(content)


def _save_to_csv(content: list, output_file: str):
    with open(output_file, "w") as f:
        for line in content:
            f.write(f"{line}\n")


def _get_loading_tids_kwargs(files, env_file, batch_size, issue, worker_count):
    kwargs_list = []
    filebatch_size = math.ceil(len(files) / worker_count)
    for i in range(0, len(files), filebatch_size):
        kwargs = {
            "files": files[i : i + filebatch_size],
            "env_file": env_file,
            "batch_size": batch_size,
            "issue": issue,
        }
        kwargs_list.append(kwargs)

    return kwargs_list


def load_uids_from_file_to_db(
    file: str,
    db_conn: db_client.DBClient,
    batch_size: int = 2048,
    env_file: str = None,
    api_count: int = 1,
):
    """
    Load uids from file to db for hydration.

    Args:
        file (str): Uid filename.
        db_conn (db_client.DBClient): Active db connection.
        batch_size (int, optional): Batch size for db updates. Defaults to 2048.
        env_file (str, optional): Dotenv file. Defaults to None.
        api_count (int, optional): Number of APIs to employ when hydrating. Defaults to 1.
    """
    settings.load_config(env_file)
    uids = _load_from_file(file)
    keys = list(range(1, api_count + 1))

    for i in range(0, len(uids), batch_size):
        uid_docs_batch = [
            {"uid": uid, "status": 0, "key": keys[i % api_count]}
            for uid in uids[i : i + batch_size]
        ]
        db_conn.add_fetching_uids(uid_docs_batch)

    if len(db_conn.fetching_uids_batch) > 0:
        db_conn._push_fetching_uids_to_db()


def load_tids_from_file_to_db(files: list, batch_size: int, env_file: str, issue: str):
    """
    Load tids to database for hydration.

    Args:
        files (list): Files of tweet ids to load from.
        batch_size (int): Batch size for db updates.
        env_file (str): Dotenv file.
        issue (str): Name of issue for hydrating process.
    """
    settings.load_config(env_file=env_file)
    db_conn = db_client.DBClient(
        issue=issue,
    )

    for idx, filepath in enumerate(files):
        tids = _load_from_file(str(filepath))
        tid_docs = [{"tid": tid, "status": 0, "file": str(filepath)} for tid in tids]

        # insert all tids in batches of size batch_size
        for i in range(0, len(tid_docs), batch_size):
            db_conn.add_hydrating_tids(tid_docs[i : i + batch_size])

        if idx % 255 == 0:
            logger.info(f"Done with {idx+1}/{len(files)} files.")

    if len(db_conn.hydrating_tids_batch) > 0:
        db_conn._push_hydrating_tids_to_db()


def load_tids_to_db(
    tids: list,
    filepath: str = "unknown",
    db_conn: db_client.DBClient = None,
    issue: str = None,
    env_file: str = None,
    batch_size: int = 4096,
    nkeys: int = 9,
):
    keys = list(range(1, nkeys + 1))
    if not db_conn:
        settings.load_config(env_file=env_file)
        db_conn = db_client.DBClient(
            issue=issue,
        )

    for i in range(0, len(tids), batch_size):
        tid_docs_batch = [
            {"tid": tid, "status": 0, "file": str(filepath), "key": keys[i % nkeys]}
            for tid in tids[i : i + batch_size]
        ]
        db_conn.add_hydrating_tids(tid_docs_batch)

    if len(db_conn.hydrating_tids_batch) > 0:
        db_conn._push_hydrating_tids_to_db()


def _load_from_file(file):
    if file.endswith(".json"):
        with open(file, "r") as f:
            elements = json.load(f)
    elif file.endswith(".jsonl"):
        elements = []
        with open(file, "r") as f:
            for line in f:
                elements.append(json.loads(line))
    elif file.endswith(".pkl"):
        with open(file, "rb") as f:
            elements = pickle.load(f)
    elif file.endswith(".txt"):
        # read each line individually
        with open(file, "r") as f:
            elements = [line.strip() for line in f]
    else:
        raise errors.SnPipelineError(errors.SnPipelineErrorMsg.UNSUPPORTED_FILE_FORMAT)

    return elements


def _get_batches(elements, batch_size=4096):
    return [elements[i : i + batch_size] for i in range(0, len(elements), batch_size)]
