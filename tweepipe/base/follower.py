import argparse
import os

import tweepy
import twint
from loguru import logger

from tweepipe import settings
from tweepipe.base import activity
from tweepipe.db import db_client
from tweepipe.utils import credentials, loader, parallel


def _fetch_followers_twint(
    uid_batch,
    output_folder: str = "./followers",
):
    """Unit method fetching followers ids for provided twitter handles batch
    using the Twint project toolkit."""

    for uid in uid_batch:
        tmp_conf = twint.Config()
        tmp_conf.Username = uid
        tmp_conf.Store_object = True

        twint.run.Followers(tmp_conf)
        followers = twint.output.follows_list

        # log user on current progress
        logger.info(f"Completed fetching of {len(followers)} followers for {uid}.")

        # save followers to json file
        output_file = f"{uid}.json"
        loader._save_to_json(followers, os.path.join(output_folder, output_file))


def _fetch_followers(
    uid_batch,
    twitter_api,
    output_folder: str = "./followers",
    verbose: bool = True,
    cap_followers: int = None,
    log_capping_file: str = "capped_users.txt",
    error_file: str = "error_users.txt",
    followers: bool = True,
    friends: bool = False,
):
    """Unit method fetching followers ids for provided twitter handles batch."""

    log_capping_file_path = os.path.join(output_folder, log_capping_file)
    error_file_path = os.path.join(output_folder, error_file)

    if followers:
        api_func = twitter_api.followers_ids
    elif friends:
        api_func = twitter_api.friends_ids
    else:
        raise ValueError("Must specify either followers or friends.")

    logger.info(f"Fetching friends for {len(uid_batch)} uids.")

    for uid in uid_batch:
        follower_cursor = tweepy.Cursor(api_func, id=uid)
        followers = []
        try:
            for page in follower_cursor.pages():
                followers.extend(page)

                if verbose and len(followers) != 0 and len(followers) % 50000 == 0:
                    logger.info(
                        f"Completed fetching of {len(followers)}/X followers for {uid}."
                    )

                # optionally cap number of followers to retrieve
                if cap_followers and len(followers) >= cap_followers:
                    logger.info(
                        f"Capping followers fetching for {uid} at {len(followers)}."
                    )
                    with open(log_capping_file_path, "a") as f:
                        f.write(str((uid, len(followers))) + "\n")
                    break
        except tweepy.error.TweepError as e:
            logger.info(f"Error while fetching {uid}.")
            with open(error_file_path, "a") as f:
                f.write(str(uid) + "\n")

        # log user on current progress
        logger.info(f"Completed fetching of {len(followers)} followers for {uid}.")

        # save followers to json file
        output_file = f"{uid}.json"
        loader._save_to_json(followers, os.path.join(output_folder, output_file))


def get_followers_parallel(
    uids,
    api_count,
    output_folder,
    twitter_credentials,
    cap_followers=None,
    batch_size=2048,
    followers=True,
    friends=False,
):
    """Organise sequential scraping for"""

    # num apis must be at least nimber of uids
    api_count = min(api_count, len(uids))
    logger.info(f"Retrieving {api_count} twitter api accesses.")

    twitter_apis, twitter_credentials = credentials._get_twitter_apis(
        twitter_credentials=twitter_credentials
    )
    print("got", len(twitter_apis), "twitter apis")
    max_workers = len(twitter_apis)
    batch_size = int(len(uids) / max_workers)

    # batch the uids to retrieve followers for
    uid_batches = loader._get_batches(uids, batch_size=batch_size)
    process_batches = activity._get_process_batches(uid_batches, twitter_apis)
    logger.info(
        f"Fetching followers for {len(uids)} uids in {len(uid_batches)} batches with {max_workers} processes."
    )

    _ = parallel.run_parallel(
        **{
            "fn": _fetch_followers,
            "kwargs_list": [
                {
                    "uid_batch": batch["uid_batch"],
                    "twitter_api": batch["twitter_api"],
                    "output_folder": output_folder,
                    "cap_followers": cap_followers,
                    "followers": followers,
                    "friends": friends,
                }
                for batch in process_batches
            ],
            "max_workers": max_workers,
        }
    )


def get_followers_sequential(
    uids,
    cap_followers,
    output_folder,
    twitter_credentials=None,
    backend="tweepy",
    followers=True,
    friends=False,
):
    """Sequentially fetch followers ids for each specified user id/screen name."""

    if not twitter_credentials:
        raise ValueError("Need twitter credentials")
    else:
        twitter_api = credentials._get_twitter_api(credentials=twitter_credentials)

    # exectue
    if backend == "tweepy":
        _fetch_followers(
            uids,
            twitter_api,
            output_folder,
            cap_followers=cap_followers,
            followers=followers,
            friends=friends,
        )
    elif backend == "twint":
        _fetch_followers_twint(uids, output_folder)
    else:
        raise ValueError


def get_followers_from_file(
    file,
    api_count: int = 1,
    twitter_credentials: list = [],
    env_file: str = None,
    parallel: bool = False,
    output_folder: str = "./followers",
    cap_followers: int = None,
    followers: bool = True,
    friends: bool = False,
):
    """Centralise followers ids retrieving process (dispatch in sequential/parallel process."""

    # use as many APIs as possible
    os.makedirs(output_folder, exist_ok=True)
    settings.load_config(env_file)

    uids = loader._load_from_file(file)
    logger.info("Loaded {} uids from file.".format(len(uids)))
    logger.info(f"Capping followers at {cap_followers}.")

    if parallel or api_count != 1 or len(twitter_credentials) > 0:
        get_followers_parallel(
            uids=uids,
            twitter_credentials=twitter_credentials,
            api_count=api_count,
            output_folder=output_folder,
            cap_followers=cap_followers,
            followers=followers,
            friends=friends,
        )
    else:
        get_followers_sequential(
            uids=uids,
            twitter_credentials=settings.TWITTER_CREDENTIALS_V1,
            output_folder=output_folder,
            cap_followers=cap_followers,
            followers=followers,
            friends=friends,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load user(s) followers.")
    parser.add_argument(
        "--file",
        type=str,
        help="File containing user ids to fetch followers for.",
        required=True,
    )
    parser.add_argument(
        "--output-folder",
        type=str,
        help="Output file.",
        required=False,
        default="./followers",
    )
    parser.add_argument(
        "--api-count",
        type=int,
        help="Number of APIs to use in the process.",
        required=False,
        default=1,
    )
    parser.add_argument(
        "--cap-followers",
        type=int,
        help="Max number of followers to retrieve.",
        required=False,
        default=1000000,
    )
    parser.add_argument(
        "--env-file",
        type=str,
        help="File containing environment variables.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "--parallel",
        type=str,
        help="Extract user profiles from streamed content.",
        required=False,
        default="no",
    )
    parser.add_argument(
        "--followers",
        type=str,
        help="Retrieve users' followers.",
        required=False,
        default="yes",
    )
    parser.add_argument(
        "--friends",
        type=str,
        help="Retrieve users' friends.",
        required=False,
        default="no",
    )

    args = parser.parse_args()

    file = args.file
    api_count = args.api_count
    parallel = args.parallel != "no"
    output_folder = args.output_folder
    env_file = args.env_file
    followers = args.followers != "no"
    friends = args.friends != "no"
    cap_followers = args.cap_followers

    get_followers_from_file(
        file=file,
        env_file=env_file,
        api_count=api_count,
        parallel=parallel,
        output_folder=output_folder,
        cap_followers=cap_followers,
        followers=followers,
        friends=friends,
    )
