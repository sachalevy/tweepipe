import json
import argparse
from pathlib import Path

from loguru import logger

from tweepipe import settings
from tweepipe.legacy import client
from tweepipe.utils import loader, tracker, credentials


def main(idx):
    settings.load_config(".env")
    keywords_file = Path("scripts/iran_protests/keywords.json")
    keywords = loader._load_from_file(str(keywords_file))

    issue = f"iran_v{idx}"
    tracker.SlackLogger.log_start_task(
        task=issue, msg=f"Streaming data for issue {issue}."
    )
    api_credentials = credentials.fetch_issue_credentials(issue.replace(f"_v{idx}", ""))

    streamer = client.Client(
        issue=issue,
        include_users=True,
        include_relations=False,
        stream=True,
        api_credentials=api_credentials,
    )

    logger.info(
        "Setup streaming data rule for keywords", json.dumps(keywords, indent=4)
    )
    logger.info(f"Starting to stream for {issue}...")
    streamer.stream(keywords=keywords)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream tweets for Iranian protests.")
    parser.add_argument(
        "--db-version",
        type=str,
        help="Database version.",
        required=True,
    )
    args = parser.parse_args()
    main(idx=args.db_version)
