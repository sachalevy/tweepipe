from tweepipe import settings
from tweepipe.legacy import client
from tweepipe.utils import credentials


def main():
    """Stream some tweets for users in english."""
    settings.load_config(env_file=".env")

    issue = "random_users_v2"
    api_credentials = credentials.fetch_issue_credentials(issue)
    standard_client = client.Client(
        issue=issue,
        include_users=True,
        include_relations=False,
        stream=True,
        api_credentials=api_credentials,
    )
    languages = ["en"]
    standard_client.stream(languages=languages)


if __name__ == "__main__":
    main()
