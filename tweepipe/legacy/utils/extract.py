import copy
import datetime
import json

from loguru import logger

from tweepipe import settings


def extract_likes(uid, tweet, db_conn):
    like_relation = get_like_relation(uid, tweet)
    db_conn.add_like_relation(like_relation)
    tweet_doc = _format_tweet_doc(tweet)
    db_conn.add_tweet(tweet_doc)


def get_like_relation(uid, tweet):
    return {
        "tid": tweet["id_str"],
        "author_uid": tweet["user"]["id_str"],
        "liking_uid": uid,
        "author_username": tweet["user"]["screen_name"],
    }


def retrieve_content_from_tweet(
    tweet, db_conn=None, include_users=True, include_relations=True, output_file=None
):
    relations = _get_relations(tweet) if include_relations else {}
    users = _get_users(tweet) if include_users else []

    # add a single tweet - original doc
    tweet_doc = _format_tweet_doc(tweet, output_file=output_file)
    if db_conn:
        db_conn.add_tweet(tweet_doc)
    elif output_file:
        add_tweet_to_file(tweet_doc, output_file)

    # add relations extracted from all subsequent docs
    if include_relations and db_conn:
        db_conn.add_relations(relations)
    # elif db_conn:
    #    db_conn.queue_relation_extraction({"tid": tweet_doc["tid"], "status": 0})

    # add users extracted in all subsequent docs
    if db_conn and include_users:
        db_conn.add_users(users)
    # TODO: add handling of user file


def add_tweet_to_file(tweet_doc: dict, output_file: str):
    logger.info(f"Got tweet {tweet_doc.get('tid')}.")
    with open(output_file, "a") as f:
        f.write(json.dumps(tweet_doc) + "\n")


def retrieve_content_from_extended_tweet():
    """Some tweets are retrieved in an extended mode - with attached extended entities."""


def _get_raw_response_doc(tweet_batch: list):
    """Transform list of responses from the Twitter API into a doc to be
    inserted in mongodb.

    The key for each of these batches is the token yielding to the next batch.
    Note that we assume the token response was appended at the end of the response
        batch.

    """

    return {tweet_batch[-1]["oldest_id"]: tweet_batch}


def _format_tweet_doc(tweet: dict, output_file: str = None):
    """From user tweet produce a tweet doc to be inserted in mongodb."""
    if not output_file:
        tweet["created_at"] = _get_creation_time_stamp(tweet["created_at"])
    tweet_doc = {
        "json": tweet,
        "tid": tweet["id_str"],
        "uid": tweet["user"]["id_str"],
    }

    return tweet_doc


def _format_user_doc(user: dict):
    """From user dict produce a user doc to be inserted in mongodb."""

    user_doc = {"json": user, "uid": user["id_str"], "screen_name": user["screen_name"]}

    return user_doc


def _get_users(tweet):
    """Retrieves all fields with user profiles specified."""

    users = [_format_user_doc(field["user"]) for field in _get_fields(tweet)]

    return users


def _get_creation_time_stamp(ts):
    try:
        return datetime.datetime.strptime(ts, settings.TWEET_TS_STR_FORMAT)
    except TypeError as e:
        return ts


def _get_fields(tweet):
    fields = [tweet]

    if "quoted_status" in tweet:
        fields.append(tweet["quoted_status"])

    if "retweeted_status" in tweet:
        fields.append(tweet["retweeted_status"])

        if "quoted_status" in tweet["retweeted_status"]:
            fields.append(tweet["retweeted_status"]["quoted_status"])

    return fields


def _get_relations(tweet):
    """Extract all relations from tweets.

    Note: updated hashtag and mention extraction from retweets. Extracting hashtags and mentions
        from a retweet means double extracting from the retweeted content, should we consider the
        retweet as producing the content itself?
    """
    relations = {"hashtag": [], "retweet": [], "mention": [], "quote": [], "reply": []}

    # NOTE: rever tto previous relations extraction pattern
    # get reply relations from tweets
    relations["reply"].extend(_get_reply_relations(tweet))
    relations["hashtag"].extend(_get_hashtag_relations(tweet))
    relations["mention"].extend(_get_mention_relations(tweet))

    # note: quoted status cannot be a retweet
    if "quoted_status" in tweet:
        try:
            relations["quote"].extend(_get_quote_relations(tweet))
        except KeyError as e:
            pass
        relations["reply"].extend(_get_reply_relations(tweet["quoted_status"]))
        relations["mention"].extend(_get_mention_relations(tweet["quoted_status"]))
        relations["hashtag"].extend(_get_hashtag_relations(tweet["quoted_status"]))

    if "retweeted_status" in tweet:
        try:
            relations["retweet"].extend(_get_retweet_relations(tweet))
        except KeyError as e:
            pass

        # extract data from original retweeted tweet
        relations["reply"].extend(_get_reply_relations(tweet["retweeted_status"]))
        relations["mention"].extend(_get_mention_relations(tweet["retweeted_status"]))
        relations["hashtag"].extend(_get_hashtag_relations(tweet["retweeted_status"]))

        # retweet can be itself a quote
        if "quoted_status" in tweet:
            try:
                relations["quote"].extend(
                    _get_quote_relations(tweet["retweeted_status"])
                )
            except KeyError as e:
                pass
            relations["reply"].extend(
                _get_reply_relations(tweet["retweeted_status"]["quoted_status"])
            )
            relations["mention"].extend(
                _get_mention_relations(tweet["retweeted_status"]["quoted_status"])
            )
            relations["hashtag"].extend(
                _get_hashtag_relations(tweet["retweeted_status"]["quoted_status"])
            )

    return relations


def _get_quote_relations(tweet):
    quote_relations = []
    created_at = _get_creation_time_stamp(tweet["created_at"])

    if "quoted_status" in tweet:
        quote_relation = {}
        quote_relation["user_id"] = tweet["user"]["id_str"]
        quote_relation["user_screen_name"] = tweet["user"]["screen_name"]

        # case where quoted tweets is no longer available (removed or user removed)
        quote_relation["quoted_user_screen_name"] = tweet["quoted_status"]["user"][
            "screen_name"
        ]
        quote_relation["quoted_user_id"] = tweet["quoted_status"]["user"]["id_str"]
        quote_relation["quoted_tweet_id"] = tweet["quoted_status"]["id_str"]
        quote_relation["tid"] = tweet["id_str"]
        quote_relation["created_at"] = created_at
        quote_relations.append(quote_relation)

    return quote_relations


def _get_retweet_relations(tweet):
    created_at = _get_creation_time_stamp(tweet["created_at"])
    retweet_relations = []

    if "retweeted_status" in tweet:
        retweet_relation = {}
        retweet_relation["user_id"] = tweet["user"]["id_str"]
        retweet_relation["user_screen_name"] = tweet["user"]["screen_name"]

        # case where user has gone missing and was not returned
        retweet_relation["retweeted_user_screen_name"] = tweet["retweeted_status"][
            "user"
        ]["screen_name"]
        retweet_relation["retweeted_user_id"] = tweet["retweeted_status"]["user"][
            "id_str"
        ]
        retweet_relation["retweet_id"] = tweet["retweeted_status"]["id_str"]

        retweet_relation["tid"] = tweet["id_str"]
        retweet_relation["created_at"] = created_at
        retweet_relations.append(retweet_relation)

    return retweet_relations


def _get_mention_relations(tweet):
    mentions = tweet["entities"]["user_mentions"]
    created_at = _get_creation_time_stamp(tweet["created_at"])

    mention_relations = []
    for mention in mentions:
        mention_relation = {}
        mention_relation["user_id"] = tweet["user"]["id_str"]
        mention_relation["user_screen_name"] = tweet["user"]["screen_name"]

        # in case has username but no user id, set flag missing to true
        try:
            mention_relation["mentioned_user_id"] = mention["id_str"]
        except KeyError as e:
            continue

        mention_relation["mentioned_user_screen_name"] = mention["screen_name"]
        mention_relation["tid"] = tweet["id_str"]
        mention_relation["created_at"] = created_at
        mention_relations.append(mention_relation)

    return mention_relations


def _get_hashtag_relations(tweet):
    hashtags = tweet["entities"]["hashtags"]
    created_at = _get_creation_time_stamp(tweet["created_at"])

    hashtag_relations = []
    for hashtag in hashtags:
        hashtag_relation = {}
        hashtag_relation["user_id"] = tweet["user"]["id_str"]
        hashtag_relation["user_screen_name"] = tweet["user"]["screen_name"]
        hashtag_relation["hashtag"] = hashtag["text"]
        hashtag_relation["tid"] = tweet["id_str"]
        hashtag_relation["created_at"] = created_at
        hashtag_relations.append(hashtag_relation)

    return hashtag_relations


def _get_reply_relations(tweet):
    reply_relations = []
    created_at = _get_creation_time_stamp(tweet["created_at"])

    if tweet["in_reply_to_status_id"] and tweet["in_reply_to_user_id"]:
        reply_relation = {}
        reply_relation["in_reply_to_tweet_id"] = tweet["in_reply_to_status_id_str"]
        reply_relation["in_reply_to_user_id"] = tweet["in_reply_to_user_id_str"]
        if "in_reply_to_screen_name" in tweet:
            reply_relation["in_reply_to_screen_name"] = tweet["in_reply_to_screen_name"]
        else:
            reply_relation["in_reply_to_screen_name"] = None
        reply_relation["tid"] = tweet["id_str"]
        reply_relation["user_id"] = tweet["user"]["id_str"]
        reply_relation["user_screen_name"] = tweet["user"]["screen_name"]
        reply_relation["created_at"] = created_at

        reply_relations.append(reply_relation)

    return reply_relations
