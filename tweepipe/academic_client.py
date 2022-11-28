import datetime
from concurrent.futures import thread

import semver
import tweepy
from loguru import logger

# make sure tweepy 4+ is available in env
assert semver.VersionInfo.parse(tweepy.__version__).major >= 4

from tweepipe import settings
from tweepipe.db import db_client, db_schema
from tweepipe.utils import streaming_client


class AcademicClient:
    """Tweepy Client dedicated to handling operations with the new
    Academic API. Compatible with tweepy 4.0.0 and above.
    """

    def __init__(
        self,
        issue: str,
        include_users: bool = True,
        include_relations: bool = True,
        schema: dict = db_schema.DEFAULT_ACADEMIC_V2_SCHEMA,
        env_file: str = None,
        stream: bool = False,
    ):
        self._env = env_file
        self.issue = issue

        self.db_conn = db_client.DBClient(
            issue=issue,
            schema=schema,
            include_relations=include_relations,
            include_users=include_users,
        )
        if not stream:
            self.tweepy_client = tweepy.Client(
                bearer_token=settings.ACADEMIC_API_BEARER_TOKEN, wait_on_rate_limit=True
            )
            self.streaming_client = None
        else:
            self.streaming_client = streaming_client.StreamingClient(
                bearer_token=settings.ACADEMIC_API_BEARER_TOKEN,
                db_conn=self.db_conn,
                wait_on_rate_limit=True,
            )
            self.tweepy_client = None

    def create_rule(self, keywords):
        if not self.streaming_client:
            raise RuntimeError("Streaming client not initialized")

        # add a rule for filtering the sample stream
        rule = tweepy.StreamRule(" ".join(keywords))
        print("Creating rule", rule)
        self.streaming_client.add_rules(rule, dry_run=True)

    def get_likes(self, uids: list, since_ts: datetime.datetime):
        # log all uids as being retrieved
        processed_uid_docs = [{"uid": uid, "status": 0} for uid in uids]
        self.db_conn.add_fetching_uids(processed_uid_docs)
        if len(self.db_conn.fetching_uids_batch) > 0:
            self.db_conn._push_fetching_uids_to_db()

        for uid in uids:
            self.db_conn.db_conn.update_one({"uid": uid}, {"$set": {"status": 1}})

            # keep going if error occurs
            try:
                likes = self.tweepy_client.get_likes(
                    uid,
                )
            except:
                pass

        return

    def stream(self):
        if not self.streaming_client:
            raise RuntimeError("Streaming client not initialized")

        # start streaming client
        self.streaming_client.sample(
            backfill_minutes=5,
            expansions=self._get_expansions(),
            user_fields=self._get_user_fields(),
            tweet_fields=self._get_user_fields(),
            threaded=True,
        )

        # not storing these for now
        # place_fields=self._get_place_fields(),
        # poll_fields=self._get_poll_fields(),

    def search_user_history(
        self,
        uid: list,
        since_ts: datetime.datetime,
        end_ts: datetime.datetime = datetime.datetime.now(),
        keywords: list = None,
    ):
        search_query = self._get_query(keywords=keywords, uid=uid)
        fetching_uids_collection = self.db_conn._get_collection(
            "fetching_uids", db_name=self.issue
        )
        fetching_uids_collection.update_one({"uid": uid}, {"$set": {"status": 1}})

        kwargs = dict(
            query=search_query,
            start_time=since_ts,
            end_time=end_ts,
            expansions=self._get_expansions(),
            max_results=100,
            media_fields=self._get_media_fields(),
            place_fields=self._get_place_fields(),
            tweet_fields=self._get_tweet_fields(),
            poll_fields=self._get_poll_fields(),
            user_fields=self._get_user_fields(),
        )
        found_tweets = self._paginate_tweets(
            self.tweepy_client.search_all_tweets, kwargs
        )
        fetching_uids_collection.update_one(
            {"uid": uid}, {"$set": {"status": 2 if found_tweets else -1}}
        )

    def search_all(
        self,
        keywords: list,
        start_time: datetime,
        end_time: datetime,
        tweets_only: bool = False,
        max_results: int = 100,
    ):
        """Run an all time search with the Twitter API."""

        search_query = self._get_query(
            keywords=keywords,
            exclude_retweet=tweets_only,
            exclude_reply=tweets_only,
            exclude_quote=tweets_only,
        )

        tweepy_client = tweepy.Client(
            bearer_token=settings.ACADEMIC_API_BEARER_TOKEN, wait_on_rate_limit=True
        )
        kwargs = dict(
            query=search_query,
            start_time=start_time,
            end_time=end_time,
            expansions=self._get_expansions(),
            max_results=max_results,
            media_fields=self._get_media_fields(),
            place_fields=self._get_place_fields(),
            poll_fields=self._get_poll_fields(),
            tweet_fields=self._get_tweet_fields(),
            user_fields=self._get_user_fields(),
        )
        _ = self._paginate_tweets(fn=tweepy_client.search_all_tweets, kwargs=kwargs)

    def _paginate_tweets(self, fn, kwargs):
        """Iterate through all pages returned by arg function."""

        response_count, retrieved_tweet_count = 0, 0
        for response in tweepy.Paginator(fn, **kwargs):
            retrieved_tweet_count += self.save_response(response)
            response_count += 1
            if response_count % 100 == 0:
                logger.info(
                    f"Retrieved {retrieved_tweet_count} tweets, {response_count} responses."
                )

        # make sure all fresh data is pushed to the database
        self.db_conn.flush_content()

        logger.info(
            f"Retrieved a total of {retrieved_tweet_count} tweets, {response_count} responses."
        )

        return response_count > 0

    def push_all(self):
        if len(self.db_conn.media_batch) > 0:
            self.db_conn._push_media_to_db()
        if len(self.db_conn.place_batch) > 0:
            self.db_conn._push_place_to_db()
        if len(self.db_conn.poll_batch) > 0:
            self.db_conn._push_poll_to_db()
        if len(self.db_conn.tweet_batch) > 0:
            self.db_conn._push_tweets_to_db()
        if len(self.db_conn.user_batch) > 0:
            self.db_conn._push_users_to_db()

    def save_response(self, response: tweepy.Response) -> int:
        """Save each response data item returned by server."""

        retrieved_tweet_count = 0
        try:
            for tweet in response.data:
                retrieved_tweet_count += 1
                try:
                    self.db_conn.add_tweet(tweet.data)
                except Exception as e:
                    logger.error(f"Error adding tweet to db. {e}")
        except Exception as e:
            logger.error(f"Error saving response. {e}")
            return

        if response.includes.get("media"):
            for media in response.includes.get("media"):
                try:
                    self.db_conn.add_media(media.data)
                except Exception as e:
                    continue

        if response.includes.get("users"):
            for user in response.includes.get("users"):
                try:
                    self.db_conn.add_user(user.data)
                except Exception as e:
                    continue

        if response.includes.get("places"):
            for place in response.includes.get("places"):
                try:
                    self.db_conn.add_place(place.data)
                except Exception as e:
                    continue

        if response.includes.get("polls"):
            for poll in response.includes.get("polls"):
                try:
                    self.db_conn.add_poll(poll.data)
                except Exception as e:
                    continue

        return retrieved_tweet_count

    def _get_query(
        self,
        keywords: list,
        uid: str = None,
        exclude_retweet: bool = False,
        exclude_quote: bool = False,
        exclude_reply: bool = False,
        query: str = "",
    ):
        """Build Academic API search query."""

        # include keywords, assuming empty query
        if keywords:
            query += " OR ".join(keywords)
            query = "( " + query + " )"

        if uid:
            query += f" from:{uid}"

        # check if should exclude queries or not
        if exclude_retweet:
            query += " -is:retweet "

        # if exclude_quote:
        #    query += " -is:quote "

        # if exclude_reply:
        #    query += " -is:reply "

        # include image requirement by default
        # query += " has:media "

        # query += " has:images "

        # has:images or has:media

        return query.strip()

    def _get_poll_fields(self):
        poll_fields = [
            "duration_minutes",
            "end_datetime",
            "id",
            "options",
            "voting_status",
        ]
        return poll_fields

    def _get_place_fields(self):
        place_fields = [
            "contained_within",
            "country",
            "country_code",
            "full_name",
            "geo",
            "id",
            "name",
            "place_type",
        ]
        return place_fields

    def _get_media_fields(self):
        media_fields = [
            "duration_ms",
            "media_key",
            "preview_image_url",
            "type",
            "url",
            "public_metrics",
            "alt_text",
        ]
        return media_fields

    def _get_tweet_fields(self):
        tweet_fields = [
            "id",
            "text",
            "attachments",
            "author_id",
            "context_annotations",
            "conversation_id",
            "created_at",
            "entities",
            "geo",
            "in_reply_to_user_id",
            "lang",
            "public_metrics",
            "possibly_sensitive",
            "referenced_tweets",
        ]

        return ",".join(tweet_fields)

    def _get_user_fields(self):
        user_fields = [
            "id",
            "name",
            "username",
            "created_at",
            "description",
            "entities",
            "location",
            "pinned_tweet_id",
            "profile_image_url",
            "protected",
            "public_metrics",
            "url",
            "verified",
        ]

        return ",".join(user_fields)

    def _get_expansions(self):
        expansions = [
            "author_id",
            "referenced_tweets.id",
            "in_reply_to_user_id",
            "attachments.media_keys",
            "attachments.poll_ids",
            "geo.place_id",
            "entities.mentions.username",
            "referenced_tweets.id.author_id",
        ]

        return ",".join(expansions)
