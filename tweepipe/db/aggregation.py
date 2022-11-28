import copy

import pymongo
from tqdm import tqdm

from tweepipe import settings
from tweepipe.db import db_client
from tweepipe.utils import snowflake


def _count_hashtag_over_tid_range(env_file, time_ranges, dbs, queries):
    """Get hashtag count over time ranges for multiple databases."""

    if env_file:
        settings.load_config(env_file=env_file)

    db_conn = db_client.DBClient()
    result_dict = {tr: {query["name"]: 0 for query in queries} for tr in time_ranges}

    for db in dbs:
        for time_range in tqdm(time_ranges):
            # recover approximative tid from date
            start_tid = snowflake.get_tweet_id_from_time(time_range[0])
            end_tid = snowflake.get_tweet_id_from_time(time_range[1])

            tid_range_query = {"tid": {"$gte": start_tid, "$lt": end_tid}}
            for query in queries:
                tmp_query = copy.deepcopy(query["query"])
                tmp_query.update(tid_range_query)

                tmp_collection = db_conn._get_collection(
                    db["collection"], db_name=db["db"]
                )

                # tmp_result = tmp_collection.count_documents(tmp_query)
                tmp_result = tmp_collection.find(
                    filter=tmp_query,
                    sort=[("tid", pymongo.ASCENDING)],
                    allow_disk_use=True,
                ).count()
                result_dict[time_range][query["name"]] += tmp_result

    return result_dict


def _aggregate_search_on_database():
    pass
