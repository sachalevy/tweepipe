import datetime
import copy

from tweepipe import settings
from tweepipe.utils.migration.mapping import (
    DataMapping,
    HashableID,
    UserAPIV1,
    UserAPIV2,
)

# classes copied from tweepy API V2 branch at
# https://github.com/tweepy/tweepy/blob/api-v2/tweepy/user.py


class UserV2(HashableID, DataMapping):

    __slots__ = (
        "data",
        "id",
        "id_str",
        "name",
        "username",
        "created_at",
        "description",
        "entities",
        "location",
        "pinned_tweet_id",
        "pinned_tweet_id_str",
        "profile_image_url",
        "protected",
        "public_metrics",
        "url",
        "verified",
        "withheld",
    )

    def __init__(self, data):
        self.data = data
        self.id_str = data["id"]
        self.id = int(self.id_str)
        self.name = data["name"]
        self.username = data["username"]

        self.created_at = data.get("created_at")
        if self.created_at is not None:
            self.created_at = datetime.datetime.strptime(
                self.created_at, "%Y-%m-%dT%H:%M:%S.%fZ"
            )

        self.description = data.get("description")
        self.entities = data.get("entities")
        self.location = data.get("location")

        self.pinned_tweet_id_str = data.get("pinned_tweet_id")
        self.pinned_tweet_id = None
        if self.pinned_tweet_id_str is not None:
            self.pinned_tweet_id = int(self.pinned_tweet_id_str)

        self.profile_image_url = data.get("profile_image_url")
        self.protected = data.get("protected")
        self.public_metrics = data.get("public_metrics")
        self.url = data.get("url")
        self.verified = data.get("verified")
        self.withheld = data.get("withheld")

    def __repr__(self):
        return f"<User id={self.id} name={self.name} username={self.username}>"

    def __str__(self):
        return self.username


class UserV1(HashableID, DataMapping):

    __slots__ = tuple(UserAPIV1.keys())
    __slots__ += ("data",)

    def __init__(self, data: dict = None, user_v2: UserV2 = None):

        self.data = data

        if user_v2 and not data:
            self._load_from_user_v2(user_v2)

    def to_dict(self):
        """Transform object to dict."""

        # data dict is available
        user_dict = dict()

        if self.data:
            user_dict = copy.deepcopy(self.data)
        else:
            ignore_slots = ["data"]
            for field in [
                slot
                for slot in UserV1.__slots__
                if self.get(slot) != None and slot not in ignore_slots
            ]:
                user_dict[field] = self.get(field)

        return user_dict

    def _load_from_user_v2(self, user_v2: UserV2):
        """Load from user v2.

        Note that we store the created_at timestamp under a string
            format which can be reloaded by other parts of the snpipeline.
        """

        self.id = user_v2.get("id")
        self.id_str = user_v2.get("id_str")
        self.name = user_v2.get("name")
        self.screen_name = user_v2.get("username")
        self.location = user_v2.get("location")
        self.url = user_v2.get("url")
        self.description = user_v2.get("description")
        self.protected = user_v2.get("protected")
        self.verified = user_v2.get("verified")

        self.followers_count = user_v2.get("public_metrics").get("followers_count")
        # friends is following, switch of terminology
        self.friends_count = user_v2.get("public_metrics").get("following_count")
        self.listed_count = user_v2.get("public_metrics").get("listed_count")
        # statuses are tweets, switch of terminology
        self.statuses_count = user_v2.get("public_metrics").get("tweet_count")

        # TODO: standardise timestamps across snpipeline
        self.created_at = user_v2.get("created_at").strftime(
            settings.TWEET_TS_STR_FORMAT
        )
        self.profile_image_url = user_v2.get("profile_image_url")

        # translator type
        # utc offset
        # time zone
        # geo enable
        # lang
        # contributor enabled
        # is translator
        # background color
        # background image url
        # background image url https
        # backgtround title
        # background link color
        # sidebar borded color
        # sidebar fill color
        # text color
        # use background imahge
        # image url https
        # banner url
        # default profile
        # default profile image
        # following
        # follow request sent
        # notifications

    def __repr__(self):
        return f"<User id={self.id} name={self.name} username={self.screen_name}>"

    def __str__(self):
        return self.screen_name
