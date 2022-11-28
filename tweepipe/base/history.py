import argparse
import json
import os
import pickle
import time
from datetime import datetime
from queue import Queue

import tweepy
from loguru import logger

from tweepipe.base import activity
from tweepipe.db import db_client
from tweepipe.legacy.utils import include
from tweepipe.utils import credentials, loader, parallel


def _get_since_timestamp(since_str):
    return int(datetime.strptime(since_str, "%Y-%m-%d").timestamp())


def _get_tweet_timestamp(created_at_str):
    return int(include._get_creation_time_stamp(created_at_str).timestamp())


def _get_user_history(
    uid=None, username=None, twitter_api=None, db_conn=None, since_ts=0
):
    kwargs = dict(include_rts=True, count=200, exclude_replies=False)
    if uid:
        kwargs["user_id"] = uid
    elif username:
        kwargs["screen_name"] = username
    else:
        raise ValueError("Either uid or username must be provided.")
    cursor = tweepy.Cursor(twitter_api.user_timeline, **kwargs)
    tweet_count = 0

    try:
        for page in cursor.pages():
            for raw_tweet in page:
                _ = include.retrieve_content_from_tweet(
                    raw_tweet._json,
                    include_users=db_conn.include_users,
                    include_relations=db_conn.include_relations,
                    db_conn=db_conn,
                )
                tweet_count += 1
    except tweepy.errors.Unauthorized:
        print(f"Unauthorized error - user {uid if uid else username}")
    except tweepy.errors.NotFound:
        print(f"User not found - user {uid if uid else username}")
    except Exception as e:
        print("Error:", e, "user:", uid if uid else username)

    return tweet_count


def _get_batch_histories(
    uid_batch,
    twitter_api,
    since="2020-08-01",
    issue="history",
    db_conn=None,
    include_relations=False,
    include_users=False,
    output_folder="./",
):
    since_ts = _get_since_timestamp(since)

    if not db_conn:
        db_conn = db_client.DBClient(
            issue=issue,
            include_relations=include_relations,
            include_users=include_users,
        )
    missing_uids = []

    for uid in uid_batch:
        try:
            # by default retrieve as much information as possible
            fetched_tweets = _get_user_history(
                uid, twitter_api, db_conn, since_ts=since_ts
            )
            logger.debug(f"Fetched {len(fetched_tweets)} tweets for {uid}.")

            output_file_path = os.path.join(
                output_folder, "tweets_" + str(uid) + ".json"
            )

            # filter out mongo oid and convert datetime to str
            for tweet in fetched_tweets:
                for u in tweet["users"]:
                    if "_id" in u:
                        del u["_id"]

                for q in tweet["relations"]["quote"]:
                    q["created_at"] = q["created_at"].strftime("%m/%d/%Y, %H:%M:%S")
                    if "_id" in q:
                        del q["_id"]

                for h in tweet["relations"]["hashtag"]:
                    h["created_at"] = h["created_at"].strftime("%m/%d/%Y, %H:%M:%S")
                    if "_id" in h:
                        del h["_id"]

                for m in tweet["relations"]["mention"]:
                    m["created_at"] = m["created_at"].strftime("%m/%d/%Y, %H:%M:%S")
                    if "_id" in m:
                        del m["_id"]

                for r in tweet["relations"]["retweet"]:
                    r["created_at"] = r["created_at"].strftime("%m/%d/%Y, %H:%M:%S")
                    if "_id" in r:
                        del r["_id"]

            with open(output_file_path, "w") as f:
                json.dump(fetched_tweets, f)

        except tweepy.error.TweepError as e:
            logger.debug(f"Encountered missing user {uid}.")
            missing_uids.append(uid)

    return missing_uids


def retrieve_user_histories(
    file=None,
    uids=None,
    issue="history",
    since="2020-08-01",
    include_users=False,
    include_relations=False,
    output_folder="./",
):
    os.makedirs(output_folder, exist_ok=True)
    db_conn = SNmongo(
        issue=issue, include_relations=include_relations, include_users=include_users
    )

    twitter_api = credentials.get_api(db_conn, purpose="lookup")[0]

    uids = loader._load_from_file(file) if not uids and file else uids

    missing_uids = _get_batch_histories(
        uids,
        twitter_api,
        db_conn=db_conn,
        issue=issue,
        include_relations=include_relations,
        include_users=include_users,
        output_folder=output_folder,
    )

    with open(os.path.join(output_folder, "missing_uids.json"), "w") as f:
        json.dump(missing_uids, f)


def retrieve_users_history_from_file(
    file,
    issue="history",
    since="2020-08-01",
    api_count=1,
    batch_size=2048,
    include_users=False,
    include_relations=False,
    output_folder="./",
    output_file="missing_users.json",
    parallel=False,
):
    output_file_path = os.path.join(output_folder, output_file)

    uids = loader._load_from_file(file)
    logger.info("Loaded {} uids from file.".format(len(uids)))

    if parallel:
        db_conn = db_client.DBClient(
            issue=issue,
            include_relations=include_relations,
            include_users=include_users,
        )

        uid_batches = loader._get_batches(uids, batch_size=batch_size)

        logger.info(f"Retrieving {api_count} twitter api accesses.")

        twitter_apis = credentials.get_api(db_conn, api_count=api_count)
        max_workers = api_count

        process_batches = activity._get_process_batches(uid_batches, twitter_apis)

        logger.info(
            f"Fetching history for {len(uids)} uids in {len(uid_batches)} batches with {max_workers} processes."
        )
        ts = time.time()
        processing_result = parallel.run_parallel(
            **{
                "fn": _get_batch_histories,
                "kwargs_list": [
                    {
                        "uid_batch": batch["uid_batch"],
                        "twitter_api": batch["twitter_api"],
                        "since": since,
                        "issue": issue,
                        "include_users": include_users,
                        "include_relations": include_relations,
                    }
                    for batch in process_batches
                ],
                "scheduler": "processes",
                "max_workers": max_workers,
                "progress": True,
                "log_done": True,
            }
        )

        missing_users = activity._get_missing_users(processing_result=processing_result)

        logger.info(
            "Found {} missing users from {} in {:.2f}.".format(
                len(missing_users), file, time.time() - ts
            )
        )
        logger.info(f"Saving missing user ids to {output_file}.")
        loader._save_to_json(content=missing_users, output_file=output_file_path)
    else:
        retrieve_user_histories(
            uids=uids,
            issue=issue,
            include_users=include_users,
            include_relations=include_relations,
            output_folder=output_folder,
        )


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Load tweets history for users.")
    parser.add_argument(
        "--file",
        type=str,
        help="File containing user ids to fetch history for.",
        required=True,
    )
    parser.add_argument(
        "--output-folder", type=str, help="Output file.", required=False, default="./"
    )
    parser.add_argument(
        "--issue",
        type=str,
        help="Name of db storing collected information.",
        required=False,
        default="history",
    )
    parser.add_argument(
        "--since",
        type=str,
        help="Specify since when should fetch history.",
        required=False,
        default="2020-08-01",
    )
    parser.add_argument(
        "--api-count",
        type=int,
        help="Number of APIs to use in the process.",
        required=False,
        default=1,
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        help="Size of active uids check batches.",
        required=False,
        default=2048,
    )
    parser.add_argument(
        "--include-relations",
        type=str,
        help="include relations from streamed content.",
        required=False,
        default="yes",
    )
    parser.add_argument(
        "--include-users",
        type=str,
        help="include user profiles from streamed content.",
        required=False,
        default="yes",
    )
    parser.add_argument(
        "--parallel",
        type=str,
        help="include user profiles from streamed content.",
        required=False,
        default="no",
    )
    parser.add_argument(
        "--reset",
        type=str,
        help="Reset twitter credentials use settings.",
        required=False,
        default="no",
    )

    args = parser.parse_args()

    file = args.file
    since = args.since
    api_count = args.api_count
    issue = args.issue
    batch_size = args.batch_size
    include_relations = args.include_relations != "no"
    include_users = args.include_users != "no"
    parallel = args.parallel != "no"
    output_folder = args.output_folder
    reset = args.reset != "no"

    if reset:
        db_conn = SNmongo()
        db_conn.reset_twitter_credentials_statuses()

    retrieve_users_history_from_file(
        file=file,
        issue=issue,
        since=since,
        api_count=api_count,
        batch_size=batch_size,
        include_relations=include_relations,
        include_users=include_users,
        parallel=parallel,
        output_folder=output_folder,
    )
