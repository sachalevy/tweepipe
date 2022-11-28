import json
import datetime

import pymongo
from tweepipe.legacy.utils import extract
import tweepy
from loguru import logger

from tweepipe import settings
from tweepipe.db import db_client, db_schema
from tweepipe.utils import parallel


def run_local_tweets_search(
    keywords: list,
    search_field: str,
    search_collection: str,
    output_issue: str,
    issue: str,
    db_conn: db_client.DBClient,
    env_file: str = None,
):
    """We search for all tweets which may contain a relevant keyword. Note that we strive
    to gather as much data as possible. If a retweet contains relevant keywords then it
    should be included here.
    """
    # assemble all user ids for which tweets have been collected
    fetched_uids = _get_uids_fetched(issue=issue, db_conn=db_conn)
    kwargs_list = _get_local_tweets_search_kwargs(
        fetched_uids=fetched_uids,
        keywords=keywords,
        search_field=search_field,
        search_collection=search_collection,
        output_issue=output_issue,
        issue=issue,
        env_file=env_file,
    )

    results = parallel.run_parallel(
        fn=_run_local_tweets_search_exec,
        kwargs_list=kwargs_list,
        max_workers=settings.WORKER_COUNT,
    )

    # sum up results of processing
    added_tweet_count, processed_tweet_count = 0, 0
    for result in results:
        added_tweet_count += result[0]
        processed_tweet_count += result[1]

    logger.info(
        f"Successfully filtered {added_tweet_count} tweets with {len(keywords)} keywords ({float(100*added_tweet_count/processed_tweet_count):.2f}%)."
    )

    return added_tweet_count, processed_tweet_count


def _run_local_tweets_search_exec(
    uids: list,
    search_collection: str,
    search_field: str,
    issue: str,
    output_issue: str,
    keywords: list,
    env_file: str = None,
    batch_size: int = 1024,
):
    if env_file:
        settings.load_config(env_file=env_file)

    input_conn = db_client.DBClient(issue=issue)
    input_collection = input_conn._get_collection(search_collection, db_name=issue)
    output_conn = db_client.DBClient(
        issue=output_issue,
        schema=db_schema.INDEX_V3,
        include_relations=True,
        include_users=True,
    )

    # re-extract data for all uids
    processed_doc_count = 0
    added_doc_count = 0
    for idx, uid in enumerate(uids):
        db_query = {"uid": uid}
        cursor = input_collection.find(
            filter=db_query,
            batch_size=batch_size,
            sort=[("uid", pymongo.ASCENDING)],
            allow_disk_use=True,
        )
        for doc in cursor:
            if is_tweet_related(
                doc["json"], keywords=keywords, search_field=search_field
            ):
                processed_tweet = extract.retrieve_content_from_tweet(
                    doc["json"],
                    include_users=output_conn.include_users,
                    include_relations=output_conn.include_relations,
                    db_conn=output_conn,
                )
                added_doc_count += 1
            processed_doc_count += 1

        if (idx + 1) % 64 == 0:
            logger.info(
                f"Finished processing {(idx+1)} users. Found {added_doc_count} related tweets ({float(100*added_doc_count/processed_doc_count):.2f}%)."
            )

    return added_doc_count, processed_doc_count


def is_tweet_related(tweet: dict, keywords: list, search_field: str = "text"):
    """Check if tweet is related to explored content."""

    def is_related(text, keywords):
        """Search subtext contained in tweet for a mentioned keyword."""
        text = text.lower().strip().replace("ー", "-")
        for keyword in keywords:
            if keyword in text:
                return True
        return False

    # produce set of keywords in lowercase for searching
    str_strip = lambda x: x.strip().lower().replace("ー", "-")
    keywords = list(map(str_strip, keywords))

    if "quoted_status" in tweet and is_related(
        tweet["quoted_status"][search_field], keywords
    ):
        return True
    if (
        "retweeted_status" in tweet
        and "quoted_status" in tweet["retweeted_status"]
        and is_related(
            tweet["retweeted_status"]["quoted_status"][search_field], keywords
        )
    ):
        return True
    if "retweeted_status" in tweet and is_related(
        tweet["retweeted_status"][search_field], keywords
    ):
        return True
    if is_related(tweet[search_field], keywords):
        return True
    else:
        return False


def _get_uids_fetched(issue: str, db_conn: db_client.DBClient):
    query = {"status": 2}
    collection = db_conn._get_collection("fetching_uids", db_name=issue)
    fetched_uid_docs = collection.find(
        filter=query, sort=[("tid", pymongo.ASCENDING)], allow_disk_use=True
    )
    fetched_uids = [doc["uid"] for doc in fetched_uid_docs]

    return fetched_uids


def _get_local_tweets_search_kwargs(
    fetched_uids: str,
    keywords: list,
    search_field: str,
    search_collection: str,
    output_issue: str,
    issue: str,
    env_file: str = None,
):
    uid_batch_size = int(len(fetched_uids) / settings.WORKER_COUNT) + 1
    uid_batches = [
        fetched_uids[i : i + uid_batch_size]
        for i in range(0, len(fetched_uids), uid_batch_size)
    ]
    kwargs_list = [
        {
            "uids": uid_batch,
            "search_collection": search_collection,
            "search_field": search_field,
            "output_issue": output_issue,
            "issue": issue,
            "keywords": keywords,
            "env_file": env_file,
        }
        for uid_batch in uid_batches
    ]

    return kwargs_list


def _get_request_parameters(
    query: str,
    start_time: str = None,
    end_time: str = None,
    since_id: str = None,
    until_id: str = None,
    tweet_fields: str = None,
    user_fields: str = None,
    expansions: str = None,
    results_per_call: int = 100,
):
    """Re-implementing searchtweets' gen_request_parameters function for lack
    of handling of second timestamps."""

    payload = {"query": query}
    # number of results to be returned
    payload["max_results"] = results_per_call

    if start_time:
        payload["start_time"] = start_time
    if end_time:
        payload["end_time"] = end_time
    if since_id:
        payload["since_id"] = since_id
    if until_id:
        payload["until_id"] = until_id
    if tweet_fields:
        payload["tweet.fields"] = tweet_fields
    if user_fields:
        payload["user.fields"] = user_fields
    if expansions:
        payload["expansions"] = expansions

    return json.dumps(payload)


def _get_fetch_from_db_histories_kwargs(
    twitter_apis: list,
    since_ts: datetime.datetime,
    env_file: str,
    issue: str,
    include_users: bool,
    include_relations: bool,
    batch_size: int = 1024,
):
    keys = list(range(1, len(twitter_apis) + 1))
    kwargs_list = []
    for api, key in zip(twitter_apis, keys):
        kwargs = {
            "twitter_api": api,
            "include_users": include_users,
            "include_relations": include_relations,
            "issue": issue,
            "env_file": env_file,
            "since_ts": since_ts,
            "batch_size": batch_size,
        }
        kwargs_list.append(kwargs)

    return kwargs_list


def _fetch_history_from_db(
    twitter_api,
    include_users,
    include_relations,
    issue,
    env_file,
    since_ts,
    batch_size,
):
    # get db connection on source & sink database
    db_conn = db_client.DBClient(
        issue=issue,
        include_relations=include_relations,
        include_users=include_users,
        schema=db_schema.INDEX_V3,
    )

    # iterate for tweet ids and lookup
    valid_uid_updates, invalid_uid_updates = [], []
    for uid in uids:
        tmp_cursor = tweepy.Cursor(
            twitter_api.user_timeline,
            id=uid,
            include_rts=True,
            count=200,
            exclude_replies=False,
            tweet_mode="extended",
        )
        try:
            iterate_history_cursor(db_conn, tmp_cursor, since_ts)
            valid_uid_updates.append(
                pymongo.operations.UpdateOne({"uid": uid}, {"$set": {"status": 2}})
            )
        except tweepy.TweepError as e:
            invalid_uid_updates.append(
                pymongo.operations.UpdateOne({"uid": uid}, {"$set": {"status": -1}})
            )

    if len(invalid_uid_updates) > 0:
        try:
            fetching_uids_collection = db_conn._get_collection(
                "fetching_uids", db_name=issue
            )
            fetching_uids_collection.bulk_write(invalid_uid_updates, ordered=False)
        except pymongo.errors.BulkWriteError as e:
            pass

    if len(valid_uid_updates) > 0:
        try:
            fetching_uids_collection = db_conn._get_collection(
                "fetching_uids", db_name=issue
            )
            fetching_uids_collection.bulk_write(valid_uid_updates, ordered=False)
        except pymongo.errors.BulkWriteError as e:
            pass


def _get_fetch_histories_kwargs(
    twitter_apis,
    twitter_credentials,
    uids,
    since_ts,
    issue,
    include_users,
    include_relations,
):
    """Produce batch of users to lookup since a given date."""

    batch_size = int(len(uids) / len(twitter_apis)) + 1
    kwargs_list = []
    for idx, i in enumerate(range(0, len(uids), batch_size)):
        uid_batch = uids[i : i + batch_size]
        twitter_api = twitter_apis[idx % len(twitter_apis)]
        twitter_credential = twitter_credentials[idx % len(twitter_apis)]
        kwargs = {
            "twitter_api": twitter_api,
            "twitter_credential": twitter_credential,
            "uids": uid_batch,
            "include_users": include_users,
            "include_relations": include_relations,
            "issue": issue,
            "since_ts": since_ts,
        }
        kwargs_list.append(kwargs)

    return kwargs_list


def _fetch_history(
    twitter_api: tweepy.API,
    twitter_credential: dict,
    uids: list,
    since_ts: datetime.datetime = None,
    issue: str = None,
    include_users: bool = True,
    include_relations: bool = True,
):
    """Lookup user histories until a given date."""

    # get db client
    db_conn = db_client.DBClient(
        issue=issue,
        include_relations=include_relations,
        include_users=include_users,
        schema=db_schema.INDEX_V3,
    )

    # push uids to db for tracking
    processed_uid_docs = [{"uid": uid, "status": 0} for uid in uids]
    db_conn.add_fetching_uids(processed_uid_docs)
    if len(db_conn.fetching_uids_batch) > 0:
        db_conn._push_fetching_uids_to_db()

    # iterate for tweet ids and lookup
    valid_uid_updates, invalid_uid_updates = [], []
    for uid in uids:
        tmp_cursor = tweepy.Cursor(
            twitter_api.user_timeline,
            id=uid,
            include_rts=True,
            count=200,
            exclude_replies=False,
            tweet_mode="extended",
        )
        try:
            iterate_history_cursor(db_conn, tmp_cursor, since_ts)
            valid_uid_updates.append(
                pymongo.operations.UpdateOne({"uid": uid}, {"$set": {"status": 2}})
            )
        except tweepy.TweepError as e:
            invalid_uid_updates.append(
                pymongo.operations.UpdateOne({"uid": uid}, {"$set": {"status": -1}})
            )

    if len(invalid_uid_updates) > 0:
        try:
            fetching_uids_collection = db_conn._get_collection(
                "fetching_uids", db_name=issue
            )
            fetching_uids_collection.bulk_write(invalid_uid_updates, ordered=False)
        except pymongo.errors.BulkWriteError as e:
            pass

    if len(valid_uid_updates) > 0:
        try:
            fetching_uids_collection = db_conn._get_collection(
                "fetching_uids", db_name=issue
            )
            fetching_uids_collection.bulk_write(valid_uid_updates, ordered=False)
        except pymongo.errors.BulkWriteError as e:
            pass


def iterate_history_cursor(
    db_conn: db_client.DBClient, cursor: tweepy.Cursor, since_ts: datetime.datetime
):
    """Iterate the history cursor to retrieve all user's tweets."""
    for page in cursor.pages():
        for raw_tweet in page:
            tweet_postdate = extract._get_creation_time_stamp(
                raw_tweet._json["created_at"]
            )
            if tweet_postdate > since_ts:
                extract.retrieve_content_from_tweet(
                    raw_tweet._json,
                    db_conn=db_conn,
                    include_users=db_conn.include_users,
                    include_relations=db_conn.include_relations,
                )
            else:
                return
    return
