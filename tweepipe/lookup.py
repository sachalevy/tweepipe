import time

import tweepy
from loguru import logger

from tweepipe import settings
from tweepipe.db import db_schema, db_client


def _get_processed_users(lookup_batch):
    return {user._json["id_str"]: user._json for user in lookup_batch}


def _lookup_users(batch, twitter_api):
    try:
        lookedup_users = twitter_api.lookup_users(user_id=batch)
    except Exception as e:
        print(f"Got following error: {e}.")
        time.sleep(3)
        return {}

    return _get_processed_users(lookedup_users)


def _lookup_users_from_uids(
    uids: list, twitter_api: tweepy.API, issue: str, env_file: str = None
):
    if env_file:
        settings.load_config(env_file=env_file)

    # setup target database with collections
    db_conn = db_client.DBClient(issue=issue, schema=db_schema.USER_LOOKUP_V1)

    missing_user_docs = []
    missing_user_count = 0
    retrieved_user_count = 0

    lookup_batch_size = 100
    for i in range(0, len(uids), lookup_batch_size):
        result = _lookup_users(uids[i : i + lookup_batch_size], twitter_api)
        docs = [{"json": result[u], "uid": u} for u in result]
        db_conn.add_users(docs)
        retrieved_user_count += len(docs)

        tmp_missing_user_docs = [{"uid": uid} for uid in result if uid not in result]
        missing_user_docs.extend(tmp_missing_user_docs)
        missing_user_count += len(tmp_missing_user_docs)

    db_conn.add_missing_users(missing_user_docs)
    logger.info(
        f"Retrieved {retrieved_user_count} users, found {missing_user_count} missing users."
    )
    logger.info(
        f"Total, got {float(retrieved_user_count/(retrieved_user_count+missing_user_count)):.2f}% of users for this batch."
    )
