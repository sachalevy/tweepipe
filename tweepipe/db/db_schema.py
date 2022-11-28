import copy

import pymongo

from tweepipe import settings

DEFAULT_TWITTER_DB_SCHEMA = {
    "api": {"api": [{"index": "access_token", "unique": True}]},
    "default": {
        "tweets": [
            {"index": "tid", "unique": True},
            {"index": "uid", "unique": False},
        ],
        "users": [
            {"index": "uid", "unique": True},
            {"index": "screen_name", "unique": False},
        ],
        "hashtag_relations": [
            {"index": "tid", "unique": False},
            {"index": "user_id", "unique": False},
            {"index": [("hashtag", pymongo.TEXT)], "unique": False},
        ],
        "mention_relations": [
            {"index": "tid", "unique": False},
            {"index": "user_id", "unique": False},
            {"index": "mentionned_user_id", "unique": False},
        ],
        "retweet_relations": [
            {"index": "tid", "unique": False},
            {"index": "user_id", "unique": False},
            {"index": "retweet_id", "unique": False},
            {"index": "retweeted_user_id", "unique": False},
        ],
        "quote_relations": [
            {"index": "tid", "unique": False},
            {"index": "user_id", "unique": False},
            {"index": "quoted_user_id", "unique": False},
            {"index": "quoted_tweet_id", "unique": False},
        ],
        "reply_relations": [
            {"index": "tid", "unique": False},
            {"index": "user_id", "unique": False},
            {"index": "in_reply_to_user_id", "unique": False},
            {"index": "in_reply_to_tweet_id", "unique": False},
        ],
        "relation_extraction": [{"index": "tid", "unique": False}],
        "fetching_uids": [{"index": "tid", "unique": False}],
    },
}

INDEX_V3 = {
    "api": {"api": [{"index": "access_token", "unique": True}]},
    "default": {
        "tweets": [
            {"index": "tid", "unique": True},
            {"index": "json.created_at", "unique": False},
            {"index": "uid", "unique": False},
        ],
        "users": [
            {"index": "uid", "unique": True},
            {"index": "screen_name", "unique": False},
        ],
        "hashtag_relations": [
            {"index": "tid", "unique": False},
            {"index": "created_at", "unique": False},
            {"index": "user_id", "unique": False},
            {"index": [("hashtag", pymongo.TEXT)], "unique": False},
        ],
        "mention_relations": [
            {"index": "tid", "unique": False},
            {"index": "created_at", "unique": False},
            {"index": "user_id", "unique": False},
            {"index": "mentionned_user_id", "unique": False},
        ],
        "retweet_relations": [
            {"index": "tid", "unique": False},
            {"index": "created_at", "unique": False},
            {"index": "user_id", "unique": False},
            {"index": "retweet_id", "unique": False},
            {"index": "retweeted_user_id", "unique": False},
        ],
        "quote_relations": [
            {"index": "tid", "unique": False},
            {"index": "created_at", "unique": False},
            {"index": "user_id", "unique": False},
            {"index": "quoted_user_id", "unique": False},
            {"index": "quoted_tweet_id", "unique": False},
        ],
        "reply_relations": [
            {"index": "tid", "unique": False},
            {"index": "created_at", "unique": False},
            {"index": "user_id", "unique": False},
            {"index": "in_reply_to_user_id", "unique": False},
            {"index": "in_reply_to_tweet_id", "unique": False},
        ],
        "relation_extraction": [{"index": "tid", "unique": False}],
        "fetching_uids": [
            {"index": "uid", "unique": True},
            {"index": "status", "unique": False},
        ],
        "hydrating_tids": [
            {"index": "tid", "unique": True},
            {"index": "key", "unique": False},
            {"index": "created_at", "unique": False},
        ],
        "likes": [
            {"index": "tid", "unique": True},
        ],
    },
}

INDEX_V3 = {
    "api": {"api": [{"index": "access_token", "unique": True}]},
    "default": {
        "tweets": [
            {"index": "tid", "unique": True},
            {"index": "json.created_at", "unique": False},
            {"index": "uid", "unique": False},
        ],
        "users": [
            {"index": "uid", "unique": True},
            {"index": "screen_name", "unique": False},
        ],
        "hashtags": [
            {"index": "tid", "unique": False},
            {"index": "created_at", "unique": False},
            {"index": "user_id", "unique": False},
            {"index": [("hashtag", pymongo.TEXT)], "unique": False},
        ],
        "mentions": [
            {"index": "tid", "unique": False},
            {"index": "created_at", "unique": False},
            {"index": "user_id", "unique": False},
            {"index": "mentionned_user_id", "unique": False},
        ],
        "retweets": [
            {"index": "tid", "unique": False},
            {"index": "created_at", "unique": False},
            {"index": "user_id", "unique": False},
            {"index": "retweet_id", "unique": False},
            {"index": "retweeted_user_id", "unique": False},
        ],
        "quotes": [
            {"index": "tid", "unique": False},
            {"index": "created_at", "unique": False},
            {"index": "user_id", "unique": False},
            {"index": "quoted_user_id", "unique": False},
            {"index": "quoted_tweet_id", "unique": False},
        ],
        "replies": [
            {"index": "tid", "unique": False},
            {"index": "created_at", "unique": False},
            {"index": "user_id", "unique": False},
            {"index": "in_reply_to_user_id", "unique": False},
            {"index": "in_reply_to_tweet_id", "unique": False},
        ],
        "relation_extraction": [{"index": "tid", "unique": False}],
        "fetching_uids": [
            {"index": "uid", "unique": True},
            {"index": "status", "unique": False},
        ],
        "hydrating_tids": [
            {"index": "tid", "unique": True},
            {"index": "key", "unique": False},
            {"index": "created_at", "unique": False},
        ],
        "likes": [
            {"index": "tid", "unique": True},
        ],
    },
}

VOTECOMPASS_DATA = {
    "default": {
        "linked_tweets": [
            {"index": "tid", "unique": True},
            {"index": "json.created_at", "unique": False},
            {"index": "uid", "unique": False},
            {"index": "status", "unique": False},
        ],
        "votecompass_result": [
            {"index": "tid", "unique": False},
            {"index": "uid", "unique": False},
            {"index": "url", "unique": False},
            {"index": "filepath", "unique": False},
        ],
    }
}


USER_LOOKUP_V1 = {
    "default": {
        "users": [
            {"index": "uid", "unique": True},
            {"index": "screen_name", "unique": False},
        ],
        "missing_users": [{"index": "uid", "unique": True}],
    }
}

BOTSPOT_V1 = {
    "default": {
        "users": [
            {"index": "uid", "unique": True},
            {"index": "username", "unique": False},
        ],
    }
}

TWITTER_RELATION_TYPES = [
    "retweet_relations",
    "hashtag_relations",
    "mention_relations",
    "quote_relations",
    "reply_relations",
]

DEFAULT_ACADEMIC_V2_SCHEMA = {
    "default": {
        "places": [{"index": "id", "unique": True}],
        "polls": [{"index": "id", "unique": True}],
        "media": [{"index": "media_key", "unique": True}],
        "tweets": [{"index": "id", "unique": True}],
        "users": [{"index": "id", "unique": True}],
        "fetching_uids": [
            {"index": "uid", "unique": True},
            {"index": "status", "unique": False},
        ],
    }
}

VOTECOMPASS_THUMBNAIL = {
    "default": {
        "places": [{"index": "id", "unique": True}],
        "polls": [{"index": "id", "unique": True}],
        "media": [{"index": "media_key", "unique": True}],
        "thumbnails": [{"index": "id", "unique": False}],
        "tweets": [{"index": "id", "unique": True}],
        "users": [{"index": "id", "unique": True}],
    }
}

LIKES_DB = {"likes": [{"index": "tid", "unique": False}]}


def _get_issue_name_from_dt(start_time, end_time, base_issue):
    """Format name based on str tmestamp."""

    start_time = "".join(i for i in start_time if i.isdigit())
    end_time = "".join(i for i in end_time if i.isdigit())

    return "{}_{}_to_{}".format(base_issue, start_time, end_time)


def _get_collection_name_from_dt(start_time: str, end_time: str):
    """Get a mirror response collection name based on two datetimes.

    Implemented for debugging purposes, will likely end up as deadcode.

    """

    collection_name = settings.RESPONSE_MIRROR_COLLECTION_NAME
    collection_name += "_{}_to_{}".format(
        "".join(i for i in start_time if i.isdigit()),
        "".join(i for i in end_time if i.isdigit()),
    )

    return collection_name


def _get_db_schema(
    issue: str = None,
    include_users: bool = True,
    include_relations: bool = True,
    schema_base: dict = DEFAULT_TWITTER_DB_SCHEMA,
):

    _db_schema = copy.deepcopy(schema_base)

    if issue:
        _db_schema[issue] = _db_schema["default"]

        if not include_users:
            del _db_schema[issue]["users"]

    return _db_schema
