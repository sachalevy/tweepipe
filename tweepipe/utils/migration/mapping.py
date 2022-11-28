import datetime
from collections.abc import Mapping
from typing import List

# classes copied from Tweepy API v2 branch at
# https://github.com/tweepy/tweepy/blob/api-v2/tweepy/mixins.py


class EqualityComparableID:
    __slots__ = ()

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.id == other.id

        return NotImplemented


class HashableID(EqualityComparableID):
    __slots__ = ()

    def __hash__(self):
        return self.id


class DataMapping(Mapping):
    __slots__ = ()

    def get(self, key):
        try:
            return getattr(self, key)
        except AttributeError:
            return None

    def __getitem__(self, key):
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError from None

    def __iter__(self):
        return iter(self.data)

    def __len__(self):
        return len(self.data)


# description of object fields post-mapping
UserAPIV2 = {
    "id": int,
    "name": str,
    "username": str,
    "created_at": str,
    "description": str,
    "entities": dict,
    "location": str,
    "pinned_tweet_id": int,
    "profile_image_url": str,
    "protected": bool,
    "public_metrics": dict,
    "url": str,
    "verified": bool,
    "withheld": dict,
}


TweetAPIV2 = {
    "id": int,
    "conversation_id": int,
    "author_id": int,
    "lang": str,
    "text": str,
    "created_at": str,
    "in_reply_to_user_id": int,
    "referenced_tweets": list,
    "attachments": list,
    "geo": dict,
    "context_annotations": list,
    "entities": {
        "annotations": list,
        "urls": list,
        "hashtags": list,
        "mentions": list,
        "cashtags": list,
    },
    "withheld": dict,
    "public_metrics": dict,
    "non_public_metrics": dict,
    "organic_metrics": dict,
    "promoted_metrics": dict,
    "possibly_sensitive": bool,
    "reply_settings": str,
    "source": str,
}


UserAPIV1 = {
    "id": int,
    "id_str": str,
    "name": str,
    "screen_name": str,
    "location": str,
    "url": str,
    "description": str,
    "translator_type": str,
    "protected": bool,
    "verified": bool,
    "followers_count": int,
    "friends_count": int,
    "listed_count": int,
    "favourites_count": int,
    "statuses_count": int,
    "created_at": str,
    "utc_offset": str,
    "time_zone": str,
    "geo_enabled": bool,
    "lang": str,
    "contributor_enabled": bool,
    "is_translator": bool,
    "profile_background_color": str,
    "profile_background_image_url": str,
    "profile_background_image_url_https": str,
    "profile_background_tile": str,
    "profile_link_color": str,
    "profile_sidebar_border_color": str,
    "profile_sidebar_fill_color": str,
    "profile_text_color": str,
    "profile_use_background_image": bool,
    "profile_image_url": str,
    "profile_image_url_https": str,
    "profile_banner_url": str,
    "default_profile": bool,
    "default_profile_image": bool,
    "following": str,
    "follow_request_sent": str,
    "notifications": str,
}

TweetAPIV1 = {
    "created_at": str,
    "id": int,
    "id_str": str,
    "text": str,
    "display_text_range": list,
    "source": str,
    "truncated": bool,
    "in_reply_to_status_id": int,
    "in_reply_to_status_id_str": str,
    "in_reply_to_user_id": int,
    "in_reply_to_user_id_str": str,
    "in_reply_to_screen_name": str,
    "user": dict,
    "geo": dict,
    "coordinates": dict,
    "place": dict,
    "contributors": dict,
    "retweeted_status": dict,
    "quoted_status": dict,
    "is_quote_status": bool,
    "quote_count": int,
    "reply_count": int,
    "retweet_count": int,
    "favorite_count": int,
    "entities": {
        "hashtags": list,
        "urls": list,
        "user_mentions": list,
        "symbols": list,
        "media": list,
    },
    "extended_entities": {
        "hashtags": list,
        "urls": list,
        "user_mentions": list,
        "symbols": list,
        "media": list,
    },
    "favorited": bool,
    "retweeted": bool,
    "possibly_sensitive": bool,
    "filter_level": str,
    "lang": str,
    "timestamp_ms": str,
}
