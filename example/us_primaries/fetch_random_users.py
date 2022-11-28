import argparse

from tweepipe import settings
from tweepipe.db import db_schema
from tweepipe.legacy import client
from tweepipe.utils import tracker, loader, credentials


def main(idx, uid_file, env_file=".env"):
    """Retrieve tweets from candidates to the US 2022 Primaries."""

    issue = f"us_primaries_random_users_2022_v{idx}"
    settings.load_config(env_file)
    settings.set_current_task(issue)
    uids = loader._load_from_file(uid_file)
    tracker.SlackLogger.log_start_task(
        task=issue, msg=f"Retrieving data for issue {issue}."
    )

    sn_client = client.Client(
        include_relations=False,
        include_users=True,
        schema=db_schema.INDEX_V3,
        issue=issue,
        api_credentials=credentials.fetch_issue_credentials(
            issue.replace(f"_v{idx}", "")
        ),
    )
    sn_client.fetch_users_timelines(uids=uids)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape politician data for US primaries."
    )
    parser.add_argument(
        "--db-version",
        type=str,
        help="Database version.",
        required=True,
    )
    parser.add_argument(
        "--env-file",
        type=str,
        help="Environment variable file.",
        required=False,
        default=".env",
    )
    parser.add_argument(
        "--uid-file",
        type=str,
        help="User id file.",
        required=False,
        default="scripts/us_primaries/random_users/uids.json",
    )
    args = parser.parse_args()
    main(idx=args.db_version, env_file=args.env_file, uid_file=args.uid_file)
