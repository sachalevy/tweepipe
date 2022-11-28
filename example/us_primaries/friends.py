from tweepipe import settings
from tweepipe.base import follower
from tweepipe.utils import credentials, tracker


def main():
    env_file = ".env"
    settings.load_config(env_file=env_file)
    uid_file = "scripts/us_primaries/random_users/uids.json"
    issue = "random_users_friends"
    api_credentials = credentials.fetch_issue_credentials(issue)
    tracker.SlackLogger.log_start_task(
        task=issue, msg=f"Streaming data for issue {issue}."
    )

    follower.get_followers_from_file(
        uid_file,
        issue=issue,
        env_file=env_file,
        twitter_credentials=[api_credentials],
        cap_followers=5000,
        parallel=False,
        friends=True,
        followers=False,
        output_folder="scripts/us_primaries/friends/",
    )


if __name__ == "__main__":
    main()
