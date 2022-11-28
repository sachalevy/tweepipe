import datetime
import copy

from tweepipe import settings
from tweepipe.utils.migration.mapping import (
    DataMapping,
    HashableID,
    TweetAPIV1,
    TweetAPIV2,
)
from tweepipe.utils.migration.versions import ApiVersion

# classes copied from Tweepy api-v2 branch at:
# https://github.com/tweepy/tweepy/blob/api-v2/tweepy/tweet.py


class TweetV2(HashableID, DataMapping):

    __slots__ = (
        "data",
        "id",
        "id_str",
        "text",
        "attachments",
        "author_id",
        "context_annotations",
        "conversation_id",
        "created_at",
        "entities",
        "geo",
        "in_reply_to_user_id",
        "in_reply_to_user_id_str",
        "lang",
        "non_public_metrics",
        "organic_metrics",
        "possibly_sensitive",
        "promoted_metrics",
        "public_metrics",
        "referenced_tweets",
        "reply_settings",
        "source",
        "withheld",
        "source_api_label",
        "author_id_str",
        "conversation_id_str",
    )

    def __init__(self, data):
        self.data = data
        self.source_api_label = ApiVersion.ACADEMIC_V2
        self.id = int(data["id"])
        self.id_str = data["id"]
        self.text = data["text"]

        self.attachments = data.get("attachments")

        self.author_id_str = data.get("author_id")
        self.author_id = None
        if self.author_id_str is not None:
            self.author_id = int(self.author_id_str)

        self.context_annotations = data.get("context_annotations", [])

        self.conversation_id_str = data.get("conversation_id")
        self.conversation_id = None
        if self.conversation_id_str is not None:
            self.conversation_id = int(self.conversation_id_str)

        self.created_at = data.get("created_at")
        if self.created_at is not None:
            self.created_at = datetime.datetime.strptime(
                self.created_at, "%Y-%m-%dT%H:%M:%S.%fZ"
            )

        self.entities = data.get("entities")
        self.geo = data.get("geo")

        self.in_reply_to_user_id_str = data.get("in_reply_to_user_id")
        self.in_reply_to_user_id = None
        if self.in_reply_to_user_id_str is not None:
            self.in_reply_to_user_id = int(self.in_reply_to_user_id_str)

        self.lang = data.get("lang")
        self.non_public_metrics = data.get("non_public_metrics")
        self.organic_metrics = data.get("organic_metrics")
        self.possibly_sensitive = data.get("possibly_sensitive")
        self.promoted_metrics = data.get("promoted_metrics")
        self.public_metrics = data.get("public_metrics")

        self.referenced_tweets = None
        if "referenced_tweets" in data:
            self.referenced_tweets = [
                ReferencedTweetV2(referenced_tweet)
                for referenced_tweet in data["referenced_tweets"]
            ]

        self.reply_settings = data.get("reply_settings")
        self.source = data.get("source")
        self.withheld = data.get("withheld")

    def __len__(self):
        return len(self.text)

    def __repr__(self):
        return f"<Tweet id={self.id} text={self.text}>"

    def __str__(self):
        return self.text


class ReferencedTweetV2(HashableID, DataMapping):

    __slots__ = ("data", "id", "id_str", "type")

    def __init__(self, data):
        self.data = data
        self.id = int(data["id"])
        self.id_str = data["id"]
        self.type = data["type"]

    def __repr__(self):
        return f"<ReferencedTweet id={self.id} type={self.type}"


class TweetV1(HashableID, DataMapping):
    """Copy TweetV2 template defined above with slots, but implement a conversion
    from the V2 format.

    Note on the difference between the Tweet from V1 and V2: upon requesting
        to include all referenced tweets in the academic v2 api product, the
        response includes responses to the tweet. In the API v1, only retweet
        and quote tweets were included, not replies. Thus, we do not take the
        replies into account while processing the tweets retrieved using the v2
        api to transform them into v1 tweets.

    """

    __slots__ = tuple(TweetAPIV1.keys())
    __slots__ += ("missing_data", "data", "source_api_label")

    def __init__(self, data: dict = None, tweet_v2: TweetV2 = None):
        # tweet v2 has been prvided and no dict data, convert the tweet

        self.missing_data = {
            "user": None,
            "quoted_status": None,
            "retweeted_status": None,
            "place": None,
        }

        self.data = data

        if tweet_v2 and not data:
            self._load_from_tweet_v2(tweet_v2)
        else:
            pass

    def to_dict(self):
        """Convert the object to dict."""

        # return already available data dict
        user_dict = dict()
        if self.data:
            user_dict = copy.deepcopy(self.data)
        else:
            ignore_slots = ["source_api_label", "missing_data", "data"]

            for field in [
                slot
                for slot in TweetV1.__slots__
                if (self.get(slot) != None and slot not in ignore_slots)
                or (slot == "in_reply_to_status_id")
            ]:
                user_dict[field] = self.get(field)

            if "retweeted_status" in user_dict:
                user_dict["retweeted_status"] = self.retweeted_status.to_dict()

            if "quoted_status" in user_dict:
                user_dict["quoted_status"] = self.quoted_status.to_dict()

            if "user" in user_dict:
                user_dict["user"] = self.user.to_dict()

        return user_dict

    def _load_from_tweet_v2(self, tweet_v2):
        """Convert tweet v2 into the v1.1 API format."""
        # init user to empty dict

        # mark this tweet with the v2 label
        self.source_api_label = ApiVersion.STANDARD_V1a1
        # convert back to string
        self.created_at = tweet_v2.get("created_at").strftime(
            settings.TWEET_TS_STR_FORMAT
        )
        self.id = tweet_v2.get("id")
        self.id_str = tweet_v2.get("id_str")
        self.lang = tweet_v2.get("lang")
        self.text = tweet_v2.get("text")
        self.display_text_range = tweet_v2.get("display_text_range")
        self.source = tweet_v2.get("source")
        self.truncated = tweet_v2.get("truncated")
        self.possibly_sensitive = tweet_v2.get("possibly_sensitive")
        self.geo = tweet_v2.get("geo")

        self.missing_data["user"] = tweet_v2.author_id

        if tweet_v2.get("geo"):
            # set the tweet's geo field
            self.geo = tweet_v2.get("geo")
            if "place_id" in self.geo:
                self.missing_data["place"] = tweet_v2.get("geo")["place_id"]
            if "coordinates" in self.geo:
                self.coordinates = self.geo["coordinates"]

        # note that retrieving these entities will contain annotations
        self.entities = {
            "hashtags": [],
            "user_mentions": [],
        }

        if isinstance(tweet_v2.get("entities"), dict):
            self.entities.update(tweet_v2.get("entities"))
            # refactor mentions to user_mentions as per v1.1

        if "mentions" in self.entities and len(self.entities["user_mentions"]) == 0:
            self.entities["user_mentions"] = self.entities["mentions"]
            del self.entities["mentions"]

            # replace new 'username' key by old 'screen_name' key
            for mention in self.entities["user_mentions"]:
                mention["screen_name"] = mention["username"]
                del mention["username"]

        # transform tag field into text field
        if "hashtags" in self.entities:
            for hashtag in self.entities["hashtags"]:
                hashtag["text"] = hashtag["tag"]
                del hashtag["tag"]

        # extended entities are the same as entities?
        # self.extended_entities = tweet_v2.get("entities")

        # these are contained in the public metrics, note favorite -> like
        self.favorite_count = tweet_v2.get("public_metrics")["like_count"]
        self.retweet_count = tweet_v2.get("public_metrics")["retweet_count"]
        self.reply_count = tweet_v2.get("public_metrics")["reply_count"]
        self.quote_count = tweet_v2.get("public_metrics")["quote_count"]

        # check if there is a reply to be found in reference tweets
        self.in_reply_to_user_id = tweet_v2.get("in_reply_to_user_id")
        self.in_reply_to_user_id_str = tweet_v2.get("in_reply_to_user_id_str")
        self.in_reply_to_status_id = None
        self.in_reply_to_status_id_str = None
        self.in_reply_to_screen_name = None

        self.is_quote_status = False
        self.retweeted_status = None
        self.quoted_status = None

        # complete reply, mention and retweet statuses
        if tweet_v2.referenced_tweets:
            for referenced_tweet in tweet_v2.referenced_tweets:
                # look for replies
                if referenced_tweet.type == "replied_to":
                    self.in_reply_to_status_id = referenced_tweet.id
                    self.in_reply_to_status_id_str = referenced_tweet.id_str

                # look for quote
                if referenced_tweet.type == "quoted":
                    self.is_quote_status = True
                    self.missing_data["quoted_status"] = referenced_tweet.id_str

                if referenced_tweet.type == "retweeted":
                    self.missing_data["retweeted_status"] = referenced_tweet.id_str

        # and those, which i'm not sure about
        self.retweeted = tweet_v2.get("retweeted")
        self.favorited = tweet_v2.get("favorited")
        self.filter_level = tweet_v2.get("filter_level")
        self.timestamp_ms = tweet_v2.get("timestamp_ms")

        # have been deprecated
        self.contributors = None

    def __len__(self):
        return len(self.text)

    def __repr__(self):
        return f"<Tweet id={self.id} text={self.text}>"

    def __str__(self):
        return self.text
