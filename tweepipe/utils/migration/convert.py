from tweepipe.utils.migration.versions import ApiVersion
from tweepipe.utils.migration.tweet import TweetV2, TweetV1
from tweepipe.utils.migration.user import UserV1, UserV2


class Converter:
    """Use the mappings defined to convert back and forth tweets between the
    v1 and v2 API formats."""

    def __init__(
        self,
    ):
        pass

    @classmethod
    def convert_to_dict(
        cls, tweets: list, version: ApiVersion = ApiVersion.STANDARD_V1a1
    ):
        """Iterate through the tweets and produce dict for each.

        In case where the dict has quoted status or a retweeted status,
            recursively convert to a dict, and set the field of the original
            dict to the new dict.

        """

        tweets_dicts = []
        for tweet in tweets:
            tweet_dict = tweet.to_dict()
            tweets_dicts.append(tweet_dict)

        return tweets_dicts

    @classmethod
    def load_tweet_references(cls, references, tweet_map):
        """Load tweet references and fill missing points in tweet map."""

        # init reference_map, check valid
        reference_map = dict()
        if "tweets" not in references:
            return tweet_map, reference_map

        # load references
        for tweet in references["tweets"]:
            tweet_v2_obj = TweetV2(tweet)
            tweet_v1_obj = TweetV1(tweet_v2=tweet_v2_obj)

            # conserve mapping of each tweet
            reference_map[tweet_v1_obj.id] = {"v2": tweet_v2_obj, "v1": tweet_v1_obj}

        # fill missing status references
        for tweet_id in tweet_map:
            for field, status_id in [
                (k, v)
                for k, v in tweet_map[tweet_id]["v1"].missing_data.items()
                if (v != None and "status" in k)
            ]:
                # if reference is available, point it to v1 object
                if status_id in reference_map:
                    setattr(
                        tweet_map[tweet_id]["v1"], field, reference_map[status_id]["v1"]
                    )

        return tweet_map, reference_map

    @classmethod
    def load_user_references(cls, references, tweet_map, reference_map):
        """Load missing user references."""

        user_map = dict()
        if "users" not in references:
            return tweet_map, user_map

        # load users from response
        for user in references["users"]:
            user_v2_obj = UserV2(user)
            user_v1_obj = UserV1(user_v2=user_v2_obj)

            user_map[user_v1_obj.id] = {"v1": user_v1_obj, "v2": user_v2_obj}

        # fill in missing users in reference tweets
        for tweet_id in reference_map:
            for field, user_id in [
                (k, v)
                for k, v in reference_map[tweet_id]["v1"].missing_data.items()
                if v != None and k == "user"
            ]:
                if user_id in user_map:
                    setattr(
                        reference_map[tweet_id]["v1"], field, user_map[user_id]["v1"]
                    )

        for tweet_id in tweet_map:
            for field, user_id in [
                (k, v)
                for k, v in tweet_map[tweet_id]["v1"].missing_data.items()
                if v != None and k == "user"
            ]:
                if user_id in user_map:
                    setattr(tweet_map[tweet_id]["v1"], field, user_map[user_id]["v1"])

        return tweet_map, user_map

    @classmethod
    def load_mentions(cls, tweet_map, user_map):
        """Load user specs from user references for missing user mentions."""

        if len(tweet_map) == 0 or len(user_map) == 0:
            return

        # fill mention ids referenced in the tweets
        for tweet_id in tweet_map:
            for mention in tweet_map[tweet_id]["v1"].entities["user_mentions"]:
                for user_id in user_map:
                    # extend mention dict with id, id_str and name fields
                    if user_map[user_id]["v1"].screen_name == mention["screen_name"]:
                        mention["id"] = user_map[user_id]["v1"].id
                        mention["id_str"] = user_map[user_id]["v1"].id_str
                        mention["name"] = user_map[user_id]["v1"].name

    @classmethod
    def load_places(cls, references, tweet_map):
        """Load places referenced by tweet."""

        if "places" not in references:
            return None

        for place in references["places"]:
            for tweet_id in tweet_map:
                for field, place_id in [
                    (k, v)
                    for k, v in tweet_map[tweet_id]["v1"].missing_data.items()
                    if v != None and k == "place"
                ]:
                    # find corresponding place reference
                    if place_id == place["id"]:
                        setattr(tweet_map[tweet_id]["v1"], field, place)

    @classmethod
    def load_replies(cls, tweet_map, user_map):
        """Load all user screen names references in tweets."""

        for tweet_id in tweet_map:
            if tweet_map[tweet_id]["v1"].in_reply_to_user_id in user_map:
                tweet_map[tweet_id]["v1"].in_reply_to_screen_name = user_map[
                    tweet_map[tweet_id]["v1"].in_reply_to_user_id
                ]["v1"].screen_name

    @classmethod
    def convert_v2_academic_restful_response_to_v1_standard(
        cls,
        response_batch: dict,
        _from: ApiVersion = ApiVersion.ACADEMIC_V2,
        _to: ApiVersion = ApiVersion.STANDARD_V1a1,
    ):
        """Convert between tweets from two different endpoints.

        The default method takes in a set of tweets and users retrieved using
            an academic api endpoint, and assembles them into v1.1 tweets, which
            include retweeted_status and quoted_status, as well as respective users.

        The expected dict should contain two keys: 'users' and 'tweets'. First sweep
            through all tweets twice, in order to get all references.

        """

        response_tweets, references = [], {}
        for response in response_batch:
            # get references to be filled
            if "users" in response:
                references = response
            elif "id" in response:
                response_tweets.append(response)

        # map of response tweets to their objects
        tweet_map = dict()
        for tweet in response_tweets:
            tweet_v2_obj = TweetV2(tweet)
            tweet_v1_obj = TweetV1(tweet_v2=tweet_v2_obj)

            # conserve mapping of each tweet
            tweet_map[tweet_v1_obj.id] = {"v2": tweet_v2_obj, "v1": tweet_v1_obj}

        # load tweet references
        tweet_map, tweet_reference_map = cls.load_tweet_references(
            references, tweet_map
        )

        # load user references
        tweet_map, user_map = cls.load_user_references(
            references, tweet_map, tweet_reference_map
        )

        # load mentions references
        cls.load_mentions(tweet_map, user_map)
        # load user screen names from references
        cls.load_replies(tweet_map, user_map)
        # load places references
        cls.load_places(references, tweet_map)

        # return a list of completed v1 tweets
        return [tweet_map[tweet_id]["v1"] for tweet_id in tweet_map]
