from datetime import datetime
import argparse

import pymongo
from pymongo import MongoClient

from tweepipe import settings
from tweepipe.botspot import botspot
from tweepipe.legacy import client


def main(db, env_file, batch_size=2048):
    """Score users from given database using the botlikelihood
    model."""

    settings.load_config(env_file=env_file)
    botspot_client = botspot.BotSpot(
        issue=db,
    )

    score_collection = botspot_client.db_conn._get_collection(
        "user_scores_botspotv1", db_name=db
    )
    score_collection.create_index("uid", unique=True)
    user_collection = botspot_client.db_conn._get_collection("users", db_name=db)
    cursor = user_collection.find(
        projection={"_id": False},
        batch_size=batch_size,
        sort=[("_id", pymongo.ASCENDING)],
    )

    user_count, user_batch = 0, []
    for user in cursor:
        user_batch.append(user["json"])
        if len(user_batch) % batch_size == 0:
            # compute users botlikelihood
            user_scores = botspot_client.get_users_scores(user_batch)
            created_at = datetime.now()

            requests = [
                pymongo.operations.InsertOne(
                    {
                        "uid": uid,
                        "score": user_scores[uid],
                        "screen_name": user["screen_name"],
                        "scored_at": created_at,
                    }
                )
                for uid, user in zip(user_scores, user_batch)
            ]

            try:
                _ = score_collection.bulk_write(requests, ordered=False)
            except pymongo.errors.BulkWriteError as e:
                pass

            user_batch = []
        user_count += 1
        if user_count % 10000 == 0:
            print(f"Scored {user_count} users.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Score a database bot likelihood.")
    parser.add_argument(
        "--env-file",
        type=str,
        help="Environment variable file.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "--db",
        type=str,
        help="Db name to score users from.",
        required=True,
    )
    args = parser.parse_args()
    main(args.db, args.env_file)
