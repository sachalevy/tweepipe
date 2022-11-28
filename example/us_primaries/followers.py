import json

from tweepipe import settings
from tweepipe.base import follower


def main():
    env_file = ".env"
    settings.load_config(env_file=env_file)
    uid_file = "scripts/us_primaries/random_users/uids.json"

    credentials_file = ".api_credentials.json"
    with open(credentials_file, "r") as f:
        twitter_credentials = json.load(f)

    follower.get_followers_from_file(
        uid_file,
        env_file=env_file,
        parallel=True,
        api_count=len(twitter_credentials),
        twitter_credentials=twitter_credentials,
        cap_followers=5000,
        output_folder="scripts/us_primaries/followers/",
    )


if __name__ == "__main__":
    main()
