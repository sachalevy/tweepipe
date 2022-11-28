from pathlib import Path
import datetime
import argparse

from tweepipe import settings, academic_client
from tweepipe.utils import loader, tracker


def main(idx):
    settings.load_config(".env")
    keywords_file = Path("scripts/iran_protests/keywords.json")
    keywords = loader._load_from_file(str(keywords_file))
    assert settings.ACADEMIC_API_BEARER_TOKEN, "Academic API bearer token is not set."

    issue = f"iran_recovered_v{idx}"
    tracker.SlackLogger.log_start_task(
        task=issue, msg=f"Starting data recovery for issue {issue}."
    )

    searcher = academic_client.AcademicClient(
        issue=issue, include_users=True, include_relations=False, stream=False
    )
    start_time = datetime.datetime(2022, 9, 12, 0, 0, 0)
    end_time = datetime.datetime(2022, 10, 18, 0, 0, 0)
    searcher.search_all(keywords=keywords, start_time=start_time, end_time=end_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape tweets with full timeline API."
    )
    parser.add_argument(
        "--db-version",
        type=str,
        help="Database version.",
        required=True,
    )
    args = parser.parse_args()
    main(idx=args.db_version)
