from tweepipe import settings
from tweepipe.db import db_schema, db_client


def get_follower_retrieval_parallel_kwargs(
    twitter_apis, uids, issue, env_file, include_relations, include_users, cap_count
):
    chunk_size = int(len(uids) / len(twitter_apis))
    kwargs_list = []
    for i in range(len(twitter_apis)):
        kwargs = {
            "twitter_api": twitter_apis[i],
            "include_users": include_users,
            "include_relations": include_relations,
            "cap_count": cap_count,
            "env_file": env_file,
            "issue": issue,
            "uids": uids[i * chunk_size : (i + 1) * chunk_size],
        }
        kwargs_list.append(kwargs)

    return kwargs_list


def fetch_followers(
    twitter_api, uids, include_users, include_relations, cap_count, env_file, issue
):
    if env_file:
        settings.load_config(env_file)

    db_conn = db_client.DBClient(
        issue=issue,
        include_relations=include_relations,
        include_users=include_users,
        schema=db_schema.INDEX_V3,
    )

    for uid in uids:
        pass
