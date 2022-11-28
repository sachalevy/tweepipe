import argparse

from loguru import logger

from tweepipe import settings
from tweepipe.db import db_schema
from tweepipe.legacy import client


def main(env_file):
    """Run the hydration process with the legacy client."""
    settings.load_config(env_file=env_file)

    datasets = ["panacea", "echen", "geocov"]
    sn_client = client.Client(
        include_relations=True, include_users=False, schema=db_schema.INDEX_V3
    )
    sn_client.set_env_file(env_file)

    for dataset in datasets:
        logger.info(f"Hydrating for {dataset}.")
        issue = f"{dataset}_keyword_study_v1"
        sn_client.set_issue(issue=issue)
        sn_client.hydrate_tweets_from_db(
            issue=issue, api_count=6, include_relations=True, include_users=False
        )


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
