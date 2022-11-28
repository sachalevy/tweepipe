from tweepipe import settings
from tweepipe.botspot import botspot
from tweepipe.legacy import client

settings.load_config(env_file=".env")


def main():
    # define our list of uids
    uids = ["800111181058838528", "1284651818782535680", "859219800"]
    apiv1 = client.Client()
    profiles = apiv1.get_user_profiles(uids, db="10_08_USAElection2020")

    # retrieve profile scores
    botspotv1 = botspot.BotSpot()
    scores = botspotv1.get_users_scores(profiles)

    print("Retrieved scores for the following users:")
    for uid in scores:
        print(f"User {uid}: {scores.get(uid)}")


if __name__ == "__main__":
    main()
