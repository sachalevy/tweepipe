import argparse
from pathlib import Path

from loguru import logger

from tweepipe import settings
from tweepipe.legacy import client
from tweepipe.utils import loader, tracker, credentials


def main(idx):
    environment_filename = ".env"
    settings.load_config(environment_filename)
    uid_file = Path("scripts/user_profiles/uids.json")
    uids = loader._load_from_file(str(uid_file))

    issue = f"user_profiles_v{idx}"
    tracker.SlackLogger.log_start_task(
        task=issue, msg=f"Streaming data for issue {issue}."
    )
    api_credentials = credentials.fetch_issue_credentials(issue.replace(f"_v{idx}", ""))

    streamer = client.Client(
        issue=issue,
        include_users=True,
        include_relations=False,
        api_credentials=api_credentials,
    )

    logger.info(f"Starting to stream for {issue}...")
    streamer.lookup_users_sequential(
        uids=uids,
        issue=issue,
        api_credentials=api_credentials,
        env_file=environment_filename,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Retrieving user profiles.")
    parser.add_argument(
        "--db-version",
        type=str,
        help="Database version.",
        required=True,
    )
    args = parser.parse_args()
    main(idx=args.db_version)
