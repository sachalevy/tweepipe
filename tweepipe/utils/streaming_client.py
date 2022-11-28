import json
from collections import namedtuple

import tweepy
from loguru import logger
from tweepy import StreamRule
from tweepy.client import Response
from tweepy.tweet import Tweet

from tweepipe.db import db_client, db_schema
from tweepipe.legacy.utils import extract

StreamResponse = namedtuple(
    "StreamResponse", ("data", "includes", "errors", "matching_rules")
)


class StreamV1(tweepy.Stream):
    def __init__(
        self,
        db_conn: db_client.DBClient,
        output_file: str,
        consumer_key: str,
        consumer_secret: str,
        access_token: str,
        access_token_secret: str,
        **kwargs,
    ):
        """
        Streaming client for the Twitter API v1.

        Args:
            db_conn (db_client.DBClient): Active db connection.
            output_file (str): Output file for data not stored to db.
            consumer_key (str): v1 API consumer key.
            consumer_secret (str): v1 API consumer secret.
            access_token (str): v1 API access token.
            access_token_secret (str): v1 API access token secret.
        """
        super().__init__(
            consumer_key, consumer_secret, access_token, access_token_secret, **kwargs
        )

        self.db_conn = db_conn
        self.output_file = output_file
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.access_token = access_token
        self.access_token_secret = access_token_secret

    def on_data(self, raw_data: str) -> bool:
        """
        Process incoming data for API v1.

        Args:
            raw_data (str): Raw data packet

        Returns:
            bool: Whether the data was correctly processed.
        """
        # shady way of extracting information from the tweet
        data = json.loads(raw_data)
        if "id_str" not in data:
            return True

        extract.retrieve_content_from_tweet(
            data,
            db_conn=self.db_conn,
            output_file=self.output_file,
            include_users=self.db_conn.include_users,
            include_relations=self.db_conn.include_relations,
        )

        return True


class StreamingClient(tweepy.StreamingClient):
    def __init__(
        self,
        bearer_token: str,
        issue: str = None,
        db_conn: db_client.DBClient = None,
        include_users: bool = False,
        include_relations: bool = False,
        return_type: type = Response,
        wait_on_rate_limit: bool = False,
        **kwargs,
    ):
        """
        _summary_

        Args:
            bearer_token (str): Twitter API v2 bearer token.
            issue (str, optional): Name of issue. Defaults to None.
            db_conn (db_client.DBClient, optional): Active db connection. Defaults to None.
            include_users (bool, optional): Whether to parse users on
                the fly. Defaults to False.
            include_relations (bool, optional): Whether to parse relations
                on the fly. Defaults to False.
            return_type (type, optional): Format of returned response from the Twitter API.
                Defaults to Response.
            wait_on_rate_limit (bool, optional): Wait when quota reached. Defaults to False.
        """
        super(tweepy.StreamingClient, self).__init__(
            bearer_token,
            return_type=return_type,
            wait_on_rate_limit=wait_on_rate_limit,
            **kwargs,
        )

        # optionally create a new database connection
        self.issue = issue
        if not db_conn:
            self.db_conn = db_client.DBClient(
                issue=issue,
                include_relations=include_relations,
                include_users=include_users,
                schema=db_schema.DEFAULT_ACADEMIC_V2_SCHEMA,
            )
        else:
            self.db_conn = db_conn

    def on_tweet(self, tweet: StreamResponse):
        """
        Store tweet to database.

        Args:
            tweet (StreamResponse): Received tweet to be stored.
        """
        self.db_conn.add_tweet(tweet.data)

    def on_data(self, raw_data: str):
        """
        Process incoming API packet.

        Args:
            raw_data (str): Raw data to be processed.
        """
        # overriding definition of on_data
        data = json.loads(raw_data)

        tweet = None
        includes = {}
        errors = []
        matching_rules = []

        if "data" in data:
            tweet = Tweet(data["data"])
            self.on_tweet(tweet)
        if "includes" in data:
            self.on_includes(includes)
        if "errors" in data:
            errors = data["errors"]
            self.on_errors(errors)
        if "matching_rules" in data:
            matching_rules = [
                StreamRule(id=rule["id"], tag=rule["tag"])
                for rule in data["matching_rules"]
            ]
            self.on_matching_rules(matching_rules)

        self.on_response(StreamResponse(tweet, includes, errors, matching_rules))

    def on_includes(self, includes: dict):
        """
        Add included attachments to db.

        Args:
            includes (dict): All attachment included in received API packet.

        Raises:
            ValueError: Unknown attachment type.
        """
        for key in includes:
            if key == "users":
                self.db_conn.add_users(includes[key])
                logger.info(
                    f"Adding {len(includes[key])} more users to the {self.issue} database."
                )
            elif key == "polls":
                self.db_conn.add_poll(includes[key])
            elif key == "media":
                self.db_conn.add_media(includes[key])
            elif key == "places":
                self.db_conn.add_place(includes[key])
            elif key == "tweets":
                self.db_conn.add_tweets(includes[key])
            else:
                raise ValueError("Unknown include key: {}".format(key))
