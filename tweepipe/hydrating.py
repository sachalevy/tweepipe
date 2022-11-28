import datetime

import pymongo
from tweepipe.legacy.utils import extract
import tweepy
from loguru import logger

from tweepipe import settings
from tweepipe.db import db_client, db_schema
from tweepipe.utils import snowflake


def _run_hydration(
    twitter_api: tweepy.API,
    tweet_ids: list,
    issue: str,
    env_file: str = None,
    include_users: bool = True,
    include_relations: bool = True,
):
    """Execute hydration process."""

    if env_file:
        settings.load_config(env_file=env_file)

    db_conn = db_client.DBClient(
        issue=issue, include_relations=include_relations, include_users=include_users
    )

    hydrated_count = 0
    processed_count = 0
    missing_count = 0
    lookup_batch_size = 100
    for i in range(0, len(tweet_ids), lookup_batch_size):
        all_lookedup_tweets = twitter_api.statuses_lookup(
            tweet_ids[i : i + lookup_batch_size], tweet_mode="extended", map_=True
        )

        missing_tids = [
            tweet._json for tweet in all_lookedup_tweets if len(tweet._json) == 1
        ]
        lookedup_tweets = [
            tweet for tweet in all_lookedup_tweets if len(tweet._json) != 1
        ]

        for lookedup_tweet in lookedup_tweets:
            processed_tweet = extract.retrieve_content_from_tweet(
                lookedup_tweet._json,
                include_users=include_users,
                include_relations=include_relations,
                db_conn=db_conn,
            )

        # store all missing tweets, potentially from suspended accounts
        db_conn.add_missing_tids(missing_tids)

        missing_count += len(missing_tids)
        hydrated_count += len(lookedup_tweets)
        processed_count += len(all_lookedup_tweets)

    if db_conn:
        db_conn.flush_content()

    return (hydrated_count, missing_count, processed_count)


def handle_db_hydration(
    twitter_api: tweepy.API,
    issue: str,
    env_file: str = None,
    include_users: bool = True,
    include_relations: bool = True,
    target_db: str = None,
    collection: str = "hydrating_tids",
    batch_size: int = 1024,
    start_date: datetime.datetime = None,
    end_date: datetime.datetime = None,
    key: int = None,
):
    # hydrate all tweets from database, if error arises log it
    while True:
        try:
            _run_hydration_from_db(
                twitter_api=twitter_api,
                issue=issue,
                env_file=env_file,
                include_users=include_users,
                include_relations=include_relations,
                target_db=target_db,
                collection=collection,
                batch_size=batch_size,
                start_date=start_date,
                end_date=end_date,
                key=key,
            )
        except Exception as e:
            logger.error(f"Error arising from hydrating {e}.")


def _run_hydration_from_db(
    twitter_api: tweepy.API,
    issue: str,
    env_file: str = None,
    include_users: bool = True,
    include_relations: bool = True,
    target_db: str = None,
    collection: str = "hydrating_tids",
    batch_size: int = 1024,
    start_date: datetime.datetime = None,
    end_date: datetime.datetime = None,
    key: int = None,
):
    """Run hydration by taking tweets from database and hydrating them"""

    if env_file:
        settings.load_config(env_file=env_file)

    if not target_db:
        target_db = issue

    logger.info(f"Hydrating tweets from {collection} in {issue} to {target_db}.")

    # setup target database with collections
    db_conn = db_client.DBClient(
        issue=target_db,
        include_relations=include_relations,
        include_users=include_users,
        schema=db_schema.INDEX_V3,
    )
    hydrating_tids_collection = db_conn._get_collection(collection, db_name=issue)

    filter = {"status": 0}
    if start_date and end_date:
        start_tid = snowflake.get_tweet_id_from_time(start_date)
        end_tid = snowflake.get_tweet_id_from_time(end_date)
        tid_range_query = {"tid_int": {"$gte": start_tid, "$lt": end_tid}}
        filter.update(tid_range_query)
    if key:
        filter.update({"key": key})

    projection = {"tid": True, "_id": True}
    logger.info(f"Filtering tweets with the key {key} - full filter : {filter}.")
    cursor = hydrating_tids_collection.find(
        filter=filter, projection=projection, batch_size=batch_size, allow_disk_use=True
    )

    success_batch, missing_batch, lookup_batch = [], [], []
    for doc in cursor:
        hydrating_tids_collection.update_one(
            {"_id": doc["_id"]}, {"$set": {"status": 1}}
        )
        lookup_batch.append(doc)

        # max lookup batch size is 100
        if len(lookup_batch) == 100:
            batch_tid_id_map = {doc["tid"]: doc["_id"] for doc in lookup_batch}
            all_lookedup_tweets = twitter_api.statuses_lookup(
                [doc["tid"] for doc in lookup_batch], tweet_mode="extended", map_=True
            )

            for tweet in all_lookedup_tweets:
                if len(tweet._json) == 1:
                    missing_batch.append(
                        pymongo.operations.UpdateOne(
                            {"_id": batch_tid_id_map[str(tweet.id)]},
                            {"$set": {"status": -1}},
                        )
                    )
                else:
                    processed_tweet = extract.retrieve_content_from_tweet(
                        tweet._json,
                        include_users=include_users,
                        include_relations=include_relations,
                        db_conn=db_conn,
                    )
                    success_batch.append(
                        pymongo.operations.UpdateOne(
                            {"_id": batch_tid_id_map[str(tweet.id)]},
                            {"$set": {"status": 2}},
                        )
                    )

            lookup_batch = []

        # if enough, update status of tweets to 'error'
        if len(missing_batch) >= batch_size:
            try:
                result = hydrating_tids_collection.bulk_write(
                    missing_batch, ordered=False
                )
            except pymongo.errors.BulkWriteError as e:
                pass

            missing_batch = []

        # same, update status of tweets to 'done'
        if len(success_batch) >= batch_size:
            try:
                result = hydrating_tids_collection.bulk_write(
                    success_batch, ordered=False
                )
            except pymongo.errors.BulkWriteError as e:
                pass

            success_batch = []

    # if enough, update status of tweets to 'error'
    if len(missing_batch) > 0:
        try:
            result = hydrating_tids_collection.bulk_write(missing_batch, ordered=False)
        except pymongo.errors.BulkWriteError as e:
            pass

        missing_batch = []

    # same, update status of tweets to 'done'
    if len(success_batch) > 0:
        try:
            result = hydrating_tids_collection.bulk_write(success_batch, ordered=False)
        except pymongo.errors.BulkWriteError as e:
            pass

        success_batch = []


def _split_time_interval(start_date, end_date, n):
    dt = (end_date - start_date) / n
    dts = [(start_date + i * dt, start_date + (i + 1) * dt) for i in range(n)]

    return dts


def _get_parallel_hydrating_from_db_kwargs(
    twitter_apis: list,
    issue: str,
    target_db: str = None,
    env_file: str = None,
    include_relations: bool = True,
    include_users: bool = True,
    batch_size: int = 4096,
    collection: str = "hydrating_tids",
    start_date: datetime.datetime = None,
    end_date: datetime.datetime = None,
):
    """Get list of keyword args for the hydration process with tids
    sourced from the database."""

    if start_date and end_date:
        # split the time periods into len(twitter_apis) pieces
        dts = _split_time_interval(start_date, end_date, len(twitter_apis))
    else:
        keys = list(range(1, 1 + len(twitter_apis)))

    logger.info(f"Preparing workload distribution on keys {keys}.")

    kwargs_list = []
    iterator = (
        zip(twitter_apis, dts) if (start_date and end_date) else zip(twitter_apis, keys)
    )
    for twitter_api, dt in iterator:
        kwargs = {
            "twitter_api": twitter_api,
            "include_users": include_users,
            "include_relations": include_relations,
            "issue": issue,
            "target_db": target_db,
            "collection": collection,
            "env_file": env_file,
            "batch_size": batch_size,
        }
        if start_date and end_date:
            kwargs.update(
                {
                    "start_date": dt[0],
                    "end_date": dt[1],
                }
            )
        else:
            kwargs.update({"key": dt})

        kwargs_list.append(kwargs)

    return kwargs_list


def _get_parallel_hydrating_kwargs(
    tweet_ids: list,
    twitter_apis: list,
    issue: str,
    env_file: str = None,
    include_relations: bool = True,
    include_users: bool = True,
    batch_size: int = 4096,
):
    """Generate arg list used to hydrate provided tweet ids."""

    kwargs_list = []
    for idx, i in enumerate(range(0, len(tweet_ids), batch_size)):
        tweet_ids_batch = tweet_ids[i : i + batch_size]
        twitter_api = twitter_apis[idx % len(twitter_apis)]
        kwargs = {
            "twitter_api": twitter_api,
            "tweet_ids": tweet_ids_batch,
            "include_users": include_users,
            "include_relations": include_relations,
            "issue": issue,
            "env_file": env_file,
        }
        kwargs_list.append(kwargs)

    return kwargs_list
