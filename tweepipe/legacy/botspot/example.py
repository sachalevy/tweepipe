from tweepipe import settings
from tweepipe.legacy import client
from tweepipe.legacy.botspot import botspot

settings.load_config(env_file=".env")


def main():
    # define our list of uids
    uids = ["800111181058838528", "1284651818782535680", "859219800"]

    issue = "my_issue"
    my_client = client.LegacyClient()
    profiles = my_client.get_user_profiles(uids, db=issue)

    # retrieve profile scores
    my_botspot = botspot.BotSpot()
    print("Retrieved scores for the following users:")
    for item in my_botspot.get_users_scores(profiles).items():
        print(f"User {item[0]}: {item[1]}")


if __name__ == "__main__":
    main()
