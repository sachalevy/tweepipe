import argparse

from tweepipe import settings
from tweepipe.legacy import client
from tweepipe.utils import tracker, credentials


def main(idx):
    """Sample 100k random users which are in located in the US, and active."""
    settings.load_config(env_file=".env")
    standard_client = client.LegacyClient(
        issue="random_users_v2",
        include_users=True,
        include_relations=False,
        stream=True,
    )
    standard_client.stream()
    settings.load_config(".env")

    issue = f"random_users{idx}"
    tracker.SlackLogger.log_start_task(
        task=issue, msg=f"Streaming data for issue {issue}."
    )
    api_credentials = credentials.fetch_issue_credentials(issue.replace(f"_v{idx}", ""))

    streamer = client.LegacyClient(
        issue=issue,
        include_users=True,
        include_relations=False,
        stream=True,
        api_credentials=api_credentials,
    )
    streamer.stream()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream tweets for the US primaries.")
    parser.add_argument(
        "--db-version",
        type=str,
        help="Database version.",
        required=True,
    )
    args = parser.parse_args()
    main(idx=args.db_version)
