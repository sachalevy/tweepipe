import argparse
from pathlib import Path

from loguru import logger

from example.covid_datasets.utils import file_iterator
from tweepipe import settings
from tweepipe.db import db_schema
from tweepipe.legacy import client


def main(env_file):
    settings.load_config(env_file=env_file)
    batch_size = 2048
    # download these files per the README
    dataset_dict = {
        "echen": file_iterator.EchenData(
            filepath=Path("scripts/covid_datasets/COVID-19-TweetIDs/2020-04/"),
            batch_size=batch_size,
        ),
        "panacea": file_iterator.PanaceaData(
            filepath=Path("scripts/covid_datasets/data/covid19_twitter/dailies/"),
            batch_size=batch_size,
        ),
        "geocov": file_iterator.GeocovData(
            filepath=Path("scripts/covid_datasets/geocov_en/"), batch_size=batch_size
        ),
    }

    sn_client = client.LegacyClient(
        include_relations=True,
        include_users=True,
        schema=db_schema.INDEX_V3,
        skip_db=False,
    )
    sn_client.set_env_file(env_file)
    for dataset in dataset_dict:
        logger.info(f"Loading all tweets for processing for {dataset}")
        # define issue database to load data to
        issue = f"{dataset}_keyword_study_v1"
        sn_client.set_issue(issue=issue)
        batch_count = 0
        # load all tweets for this dataset on month of april to db
        for tweet_batch in dataset_dict.get(dataset):
            if not tweet_batch:
                break

            # update tweet_batch with api credentials keys for load balancing
            tweet_batch = balance_key_load(tweet_batch)
            sn_client.db_conn.add_hydrating_tids(tweet_batch)

            # notify of progress
            batch_count += 1
            if batch_count % 1024 == 0:
                logger.info(
                    f"Have loaded {batch_count * batch_size} tweet ids in {issue}."
                )

        # push all remaining tids for this database
        if len(sn_client.db_conn.hydrating_tids_batch) > 0:
            sn_client.db_conn._push_hydrating_tids_to_db()


def balance_key_load(tweet_batch, key_count=6):
    """Make sure different keys handle different tweet ids

    This is helpful when spreading the data collection across APIs.
    """

    key_idxs = list(range(1, key_count + 1))
    for i, tweet in enumerate(tweet_batch):
        tweet["key"] = key_idxs[i % key_count]
    return tweet_batch


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Hydrate tweets from large scale datasets."
    )
    parser.add_argument(
        "--env-file",
        type=str,
        help="Environment variable file.",
        required=False,
        default=None,
    )

    args = parser.parse_args()
    main(
        env_file=args.env_file,
    )
