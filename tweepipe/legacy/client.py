import datetime
import json
import random
import requests
import semver
from tweepipe.legacy.utils import extract


random.seed(123)

import searchtweets
import yaml
import tweepy
from tqdm import tqdm
import pymongo
from loguru import logger
from omegaconf import OmegaConf

# can only import streamlistener if tweepy is previous to 4.0
if semver.VersionInfo.parse(tweepy.__version__).major < 4:
    from tweepipe import streaming

from tweepipe import hydrating, settings, searching, lookup
from tweepipe.db import db_client, db_schema
from tweepipe.utils import (
    credentials,
    loader,
    parallel,
    streaming_client,
)
from tweepipe.base import history
from tweepipe.utils.migration import convert, versions
from tweepipe.utils.snowflake import SnowFlake


class LegacyClient:
    """
    Legacy client to work with Tweepy < 4.0.0. Compatible with the tweepipe MongoDB client.

    :param issue: Name of the ongoing issue, also the working database. Defaults to None.
    :type issue: str, optional
    :param include_users: Whether to extract user profiles upon processing the retrieved tweets.
        If so, the users will be stored in a separate collection. Defaults to True.
    :type include_users: bool, optional
    :param include_relations: Whether to extract relations (hashtags, quotes, mentions, retweets)
        upon processing. Like users, if extracted they will be stored under separate collections
        in the mongo database. Defaults to True.
    :type include_relations: bool, optional
    :param skip_db: Set to True to skip usage of a MongoDB, and not have persistent storage on
        collection tasks. Defaults to False.
    :type skip_db: bool, optional
    :param schema: Specify an index schema for the retrieved Twitter data. For example, an index
        on tweet ids can be useful to quickly get to a tweet. Defaults to db_schema.INDEX_V3.
    :type schema: dict, optional
    """

    def __init__(
        self,
        issue: str = None,
        include_users: bool = True,
        include_relations: bool = True,
        skip_db: bool = False,
        schema: dict = None,
        stream: bool = False,
        api_credentials: dict = None,
    ):

        self._issue = issue
        self._include_users = include_users
        self._include_relations = include_relations
        self._schema = schema if schema else db_schema.INDEX_V3

        try:
            if not skip_db:
                self._db_conn = db_client.DBClient(
                    issue=self._issue,
                    include_users=self._include_users,
                    include_relations=self._include_relations,
                    schema=self._schema,
                )
            else:
                self._db_conn = None
        except Exception as e:
            logger.error(f"Encountered error establishing db connection {e}")
            self._db_conn = None
            pass

        self._env_file = None

        if stream and not skip_db:
            # get credentials from environment
            # TODO: link to mongodb to pull credentials from there

            if not api_credentials:
                api_credentials = dict(
                    consumer_key=settings.CONSUMER_KEY,
                    consumer_secret=settings.CONSUMER_SECRET,
                    access_token=settings.ACCESS_TOKEN,
                    access_token_secret=settings.ACCESS_TOKEN_SECRET,
                )

            self.streaming_client = streaming_client.StreamV1(
                db_conn=self._db_conn, output_file="stream.json", **api_credentials
            )
        else:
            self.streaming_client = None

    def stream(self, keywords: list = None, languages: list = None):
        if not self.streaming_client:
            raise RuntimeError("Streaming client not initialized")

        # run a sampling stream, directed towards db
        if keywords:
            self.streaming_client.filter(track=keywords)
        elif languages:
            self.streaming_client.sample(languages=languages)
        else:
            self.streaming_client.sample()

    def create_rule(self, keywords):
        if not self.streaming_client:
            raise RuntimeError("Streaming client not initialized")

        # add a rule for filtering the sample stream
        rule = tweepy.StreamRule(" ".join(keywords))
        self.streaming_client.add_rules(rule)

    def stream_twitter_feed_seq(
        self,
        keywords: list = [],
        keywords_file: str = None,
        issue: str = None,
        twitter_credentials: dict = None,
        output_file: str = None,
        skip_db: bool = False,
    ):
        if len(keywords) == 0 and keywords_file:
            keywords = loader._load_from_file(keywords_file)

        tmp_issue = self._get_issue(issue=issue)

        if not twitter_credentials:
            twitter_credentials = credentials._get_twitter_credentials(
                db_conn=self.db_conn, api_count=1, purpose="stream"
            )[0]

        logger.info(
            f"Retrieved {twitter_credentials['consumer_key']} twitter authentication for sequential streaming."
        )

        streaming.stream_keywords(
            keywords=keywords,
            twitter_credentials=twitter_credentials,
            issue=tmp_issue,
            output_file=output_file,
            include_users=self.include_users,
            include_relations=self.include_relations,
            skip_db=skip_db,
        )

    def get_user_profiles(self, uids: list, db: str, collection: str = "users"):
        """Retrieve user profiles."""

        user_profiles = []
        target_collection = self.db_conn._get_collection(collection, db_name=db)
        projection = {"_id": False}
        for uid in uids:
            query = {"uid": uid}
            tmp_user_profile = target_collection.find_one(
                filter=query, projection=projection
            )
            if tmp_user_profile:
                user_profiles.append(tmp_user_profile.get("json"))

        return user_profiles

    def stream_twitter_feed(
        self,
        keywords: list = [],
        keywords_file: str = None,
        issue: str = None,
        api_count: int = 1,
        twitter_credentials: list = None,
    ):
        """Stream twitter feed using Twitter APIs. The default execution uses a single process.

        To perform parallel streaming, specify an api_count greater than 1. To employ all
            available apis, set api_count to -1.
        """

        if len(keywords) == 0 and keywords_file:
            keywords = loader._load_from_file(keywords_file)

        # retrieve issue + twitter credentials
        tmp_issue = self._get_issue(issue=issue)
        if not twitter_credentials:
            twitter_credentials = credentials._get_twitter_credentials(
                db_conn=self.db_conn, api_count=api_count, purpose="stream"
            )
        twitter_auths = credentials._get_twitter_auths(twitter_credentials)
        logger.info(f"Retrieved {len(twitter_auths)} twitter authentication insts.")

        # run parallel streamers by batching keywords
        process_count = len(twitter_auths)
        kwargs_list = streaming._get_parallel_streaming_kwargs(
            keywords=keywords,
            twitter_auths=twitter_auths,
            process_count=process_count,
            issue=tmp_issue,
            include_users=self.include_users,
            include_relations=self.include_relations,
        )
        results = parallel.run_parallel(
            fn=streaming._run_streamer,
            kwargs_list=kwargs_list,
            max_workers=process_count,
        )

    def get_likes(self, uids: list, since_ts: datetime.datetime, api_count: int = 1):
        # log all uids as being retrieved
        processed_uid_docs = [{"uid": uid, "status": 0} for uid in uids]
        self.db_conn.add_fetching_uids(processed_uid_docs)
        if len(self.db_conn.fetching_uids_batch) > 0:
            self.db_conn._push_fetching_uids_to_db()

        twitter_apis, twitter_credentials = credentials._get_twitter_apis(
            db_conn=self.db_conn,
            api_count=1,
            purpose="hydrate",
            free_api=True,
        )
        twitter_api = twitter_apis[0]

        since_tid = SnowFlake.get_tweet_id_from_time(since_ts)

        fetching_uids = self.db_conn._get_collection("fetching_uids")

        for uid in tqdm(uids):
            fetching_uids.update_one({"uid": uid}, {"$set": {"status": 1}})
            # keep going if error occurs
            try:
                for page in tweepy.Cursor(
                    twitter_api.favorites,
                    id=uid,
                    count=100,
                    since_id=since_tid,
                    tweet_mode="extended",
                ).pages():
                    for status in page:
                        extract.extract_likes(uid, status._json, self.db_conn)
                fetching_uids.update_one({"uid": uid}, {"$set": {"status": 2}})
            except Exception as e:
                print(e)
                fetching_uids.update_one({"uid": uid}, {"$set": {"status": -1}})
                pass

        self.db_conn.flush_content()

        return

    def get_user_friends(
        self,
    ):
        """Retrieve friends for a single user."""

    def get_users_friends(
        self,
    ):
        """Retrieve friends for multiple users."""

    def fetch_followers(
        self,
        uids: list,
        free_api: bool = False,
        api_count: int = 1,
        cap_count: int = 50000,
    ):
        pass

    def load_tweet_files_to_hydrate(
        self, files: list, batch_size: int = 4096, worker_count: int = 6
    ):
        """Load tweet ids from files into collection for hydration.

        Use a status to get tids to hydrate:
            0: waiting
            1: hydrating
            2: done
            -1: missing
        """

        # make sure hydrating index exists
        self.db_conn.create_hydrating_index()

        kwargs_list = loader._get_loading_tids_kwargs(
            files=files,
            batch_size=batch_size,
            env_file=self.env_file,
            issue=self._get_issue(),
            worker_count=worker_count,
        )

        # loader.load_tids_from_file_to_db(**kwargs_list.pop())
        results = parallel.run_parallel(
            fn=loader.load_tids_from_file_to_db,
            kwargs_list=kwargs_list,
            max_workers=worker_count,
        )

    def hydrate_tweets_from_db(
        self,
        start_date: datetime.datetime = None,
        end_date: datetime.datetime = None,
        api_count: int = 1,
        issue: str = None,
        target_db: str = None,
        batch_size: int = 1024,
        include_relations: bool = True,
        include_users: bool = True,
        collection: str = "hydrating_tids",
        free_api: bool = False,
    ):
        """Same as the hydrate tweets method, however take all tids from
            database instead of taking them from a file or input list.

        Note that the current hydrating tids for the usc elections database
            use a field tid_int which stores all tweet ids as integers additionally
            to the tid field which stores the tweet ids as strings. All additional
            id fields are now moved to integers for simpler searches.
        """

        include_users = self._get_include_users(include_users)
        include_relations = self._get_include_relations(include_relations)

        tmp_issue = self._get_issue(issue=issue)
        target_db = tmp_issue if not target_db else target_db
        logger.info(
            f"Hydrating data from {tmp_issue}. Include users: {include_users}. Include relations {include_relations}."
        )

        twitter_apis, twitter_credentials = credentials._get_twitter_apis(
            db_conn=self.db_conn,
            api_count=api_count,
            purpose="hydrate",
            free_api=free_api,
        )
        logger.info(f"Retrieved {len(twitter_apis)} api accesses.")
        if len(twitter_apis) == 1:
            twitter_api = twitter_apis.pop()
            logger.info(
                f"Starting hydrating using API access with key {twitter_credentials[0].get('consumer_key')}."
            )
            results = hydrating._run_hydration_from_db(
                twitter_api=twitter_api,
                include_relations=include_relations,
                include_users=include_users,
                target_db=target_db,
                issue=tmp_issue,
                collection=collection,
                env_file=self.env_file,
                batch_size=batch_size,
                start_date=start_date,
                end_date=end_date,
            )
        else:
            kwargs_list = hydrating._get_parallel_hydrating_from_db_kwargs(
                twitter_apis=twitter_apis,
                issue=tmp_issue,
                target_db=target_db,
                include_users=include_users,
                include_relations=include_relations,
                env_file=self.env_file,
                collection=collection,
                start_date=start_date,
                end_date=end_date,
            )
            results = parallel.run_parallel(
                fn=hydrating._run_hydration_from_db,
                kwargs_list=kwargs_list,
                max_workers=len(twitter_apis),
            )

        return results

    def hydrate_tweets(
        self,
        tweet_ids: list = [],
        tweet_ids_file: str = None,
        issue: str = None,
        api_count: int = 1,
    ):
        """Recover tweet contents from tweet ids."""

        if tweet_ids_file:
            tweet_ids = loader._load_from_file(tweet_ids_file)

        tmp_issue = self._get_issue(issue=issue)

        # retrieve twitter apis
        twitter_apis, _ = credentials._get_twitter_apis(
            db_conn=self.db_conn, api_count=api_count, purpose="hydrate"
        )

        # run sequential hydration process
        if len(twitter_apis) == 1:
            twitter_api = twitter_apis.pop()
            results = hydrating._run_hydration(
                twitter_api=twitter_api,
                tweet_ids=tweet_ids,
                issue=tmp_issue,
                include_users=self.include_users,
                include_relations=self.include_relations,
            )
        # run parallel streamers by batching keywords
        else:
            process_count = len(twitter_apis)
            kwargs_list = hydrating._get_parallel_hydrating_kwargs(
                tweet_ids=tweet_ids,
                twitter_apis=twitter_apis,
                issue=tmp_issue,
                env_file=self.env_file,
                include_relations=self.include_relations,
                include_users=self.include_users,
            )
            # in results is returned lists of hydrated tweets
            results = parallel.run_parallel(
                fn=hydrating._run_hydration,
                kwargs_list=kwargs_list,
                max_workers=process_count,
            )

        return results

    def fetch_user_history(self, uid):
        """Recover a user's tweets timeline."""

    def search_users_histories(
        self,
        uids: list,
        since_ts: datetime.datetime,
        end_ts: datetime.datetime = datetime.datetime.now(),
        issue: str = None,
        keywords: list = None,
        include_users: bool = True,
        include_relations: bool = True,
    ):
        issue = self._get_issue(issue=issue)
        include_users = self._get_include_users(include_users=include_users)
        include_relations = self._get_include_relations(
            include_relations=include_relations
        )

        # log all uids to be scraped in this process
        processed_uid_docs = [{"uid": uid, "status": 0} for uid in uids]
        self.db_conn.add_fetching_uids(processed_uid_docs)
        if len(self.db_conn.fetching_uids_batch) > 0:
            self.db_conn._push_fetching_uids_to_db()

        valid_uid_updates, invalid_uid_updates = [], []

        # get collection for uids being fetched
        fetching_uids_collection = self.db_conn._get_collection(
            "fetching_uids", db_name=issue
        )
        to_fetch_uids = list(fetching_uids_collection.find(filter={"status": 0}))
        logger.info(f"Searching for {len(to_fetch_uids)} uids using Academic API.")
        academic_credentials = self._get_academic_credentials()
        for uid in tqdm(to_fetch_uids):
            raw_output_filename, output_filename = self._get_output_filepaths(
                start_time=since_ts, end_time=end_ts, uid=uid
            )
            try:
                logger.info(f"Searching for {uid['uid']}")
                found_tweets = self._run_full_archive_continuous_search(
                    raw_output_filename=raw_output_filename,
                    output_filename=output_filename,
                    credentials=academic_credentials,
                    uid=uid["uid"],
                    keywords=keywords,
                    start_time=since_ts,
                    end_time=end_ts,
                )
            except requests.exceptions.HTTPError as e:
                print(e)
                logger.error(e)
                found_tweets = False

            if found_tweets:
                fetching_uids_collection.update_one(
                    {"uid": uid}, {"$set": {"status": 2}}
                )
            else:
                fetching_uids_collection.update_one(
                    {"uid": uid}, {"$set": {"status": -1}}
                )

    def fetch_users_from_db_histories(
        self,
        since_ts: datetime.datetime = None,
        api_count: int = 1,
        free_api: bool = False,
        api_purpose: str = "lookup",
    ):
        logger.info(
            f"Starting lookup process on {self.issue}. Include relations: {self.include_relations}. Include users: {self.include_users}."
        )
        twitter_apis, twitter_credentials = credentials._get_twitter_apis(
            db_conn=self.db_conn,
            api_count=api_count,
            purpose=api_purpose,
            free_api=free_api,
        )

        kwargs_list = searching._get_fetch_from_db_histories_kwargs(
            twitter_apis=twitter_apis,
            since_ts=since_ts,
            issue=tmp_issue,
            include_users=include_users,
            include_relations=include_relations,
        )

        results = parallel.run_parallel(
            fn=searching._fetch_history,
            kwargs_list=kwargs_list,
            max_workers=len(twitter_apis),
        )

        credentials.free_api(
            db_conn=self.db_conn,
            twitter_credentials=twitter_credentials,
            purpose=api_purpose,
        )

    def fetch_users_histories(
        self,
        uids: list,
        since_ts: datetime.datetime,
        api_count: int = -1,
        issue: str = None,
        free_api: bool = False,
        include_users: bool = True,
        include_relations: bool = True,
        twitter_credentials: list = None,
        api_purpose: str = "lookup",
    ):
        """Recover multiple users tweets timelines."""

        include_users = self._get_include_users(include_users)
        include_relations = self._get_include_relations(include_relations)
        tmp_issue = self._get_issue(issue=issue)
        logger.info(
            f"Starting lookup process on {tmp_issue}. Include relations: {include_relations}. Include users: {include_users}."
        )
        twitter_apis, twitter_credentials = credentials._get_twitter_apis(
            db_conn=self.db_conn,
            api_count=api_count,
            purpose=api_purpose,
            twitter_credentials=twitter_credentials,
            free_api=free_api,
        )

        logger.info(
            f"Looking up {len(uids)} users with {[cred['consumer_key'] for cred in twitter_credentials]} apis."
        )

        kwargs_list = searching._get_fetch_histories_kwargs(
            twitter_apis=twitter_apis,
            twitter_credentials=twitter_credentials,
            uids=uids,
            since_ts=since_ts,
            issue=tmp_issue,
            include_users=include_users,
            include_relations=include_relations,
        )

        results = parallel.run_parallel(
            fn=searching._fetch_history,
            kwargs_list=kwargs_list,
            max_workers=len(twitter_apis),
        )

        credentials.free_api(
            db_conn=self.db_conn,
            twitter_credentials=twitter_credentials,
            purpose=api_purpose,
        )

    def lookup_users_sequential(
        self,
        uids: list = [],
        issue: str = None,
        api_credentials: dict = None,
        env_file: str = None,
    ):
        twitter_api = credentials._get_twitter_api(credentials=api_credentials)
        print("Collecting data from Twitter API.")
        lookup._lookup_users_from_uids(
            uids=uids, twitter_api=twitter_api, issue=issue, env_file=env_file
        )

    def lookup_users(self, uids: list = [], issue: str = None, api_count: int = -1):
        """Lookup user profiles and insert them in database."""

        tmp_issue = self._get_issue(issue=issue)
        twitter_apis, _ = credentials._get_twitter_apis(
            db_conn=self.db_conn, api_count=api_count, purpose="lookup"
        )
        kwargs_list = parallel.get_lookup_users_kwargs_list(
            uids=uids,
            apis=twitter_apis,
            env_file=self.env_file,
            issue=tmp_issue,
        )
        logger.info(
            f"Preparing data retrieval for {len(kwargs_list)} user batches and {len(twitter_apis)} twitter apis."
        )
        results = parallel.run_parallel(
            fn=lookup._lookup_users_from_uids,
            kwargs_list=kwargs_list,
            max_workers=len(twitter_apis),
        )

    def check_user_activity(
        self,
    ):
        """Verify whether or not a user is active."""

    def check_users_activity(
        self,
    ):
        """Verify whether or not multiple users are active."""

    def get_botspot_score(
        self,
    ):
        """Score bot-likelihood of a user using botspot."""

    def lookup_tweets(self):
        """Basically the same thing as hydrate tweet but only for a single tweet id."""

    def recover_tweets(
        self,
        keywords: list = [],
        start_time: datetime.datetime = None,
        end_time: datetime.datetime = None,
        use_academic_api: bool = True,
        twitter_credentials: list = None,
        sampling_rate: int = 1,
        full: bool = False,
        bbox: str = None,
        country: str = None,
    ):
        """Search tweets from a given period of time.

        Args:
            keywords:       Keywords to use for data recovery.
            start_time: Datetime timestamp designating begining of missing data period.
            end_time:   Datetime timestamp designating end of missing data period.
            use_academic_api:       Request access to an academic api to perform the tweet
                    retrieval. This is required if getting tweets more than a week after they
                    were posted (limit of the standard api).
            expected_proportion: Depending on the purpose of the tweets recovery, one may
                want to reproduce the filtering process used by Twitter upon streaming tweets
                with the free API (yielding 1% of the total stream).

        If the client wishes to use an academic API (early access v2 at this point), the full
            archive search api will return a complete set of tweets corresponding to the query
            for the given time period. The result of this query will then be stored and saved
            for later processing. If the client is striving to complete a dataset, we assume that
            1% of the retrieved tweets are actually missing. We thus, sample the database of
            retrieved tweets to obtain a representative set of tweets.

        """

        if not use_academic_api and not self._is_valid_timestamp(start_time):
            raise ValueError("The timestamp goes back more than 7 days!")

        # use the searchtweets handler to query twitter's full archive
        if use_academic_api:
            self._full_archive_tweets_search(
                keywords=keywords,
                start_time=start_time,
                end_time=end_time,
                onfly_sampling=(not full),
                sampling_rate=sampling_rate,
                bbox=bbox,
                country=country,
            )
        # otherwise implement recent tweets search
        else:
            self._recent_tweets_search(
                keywords=keywords,
                twitter_credentials=twitter_credentials,
                start_time=start_time,
                end_time=end_time,
                bbox=bbox,
                country=country,
            )

    def _recent_tweets_search(
        self,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        twitter_credentials: list = None,
        keywords: list = None,
        bbox: str = None,
        country: str = None,
    ):
        twitter_api = credentials._get_twitter_api(
            db_conn=self._db_conn, purpose="search", credentials=twitter_credentials[0]
        )
        query = self._get_twitter_api_query(
            keywords=keywords, bbox=bbox, country=country
        )
        logger.info(f"Working on recent search with query {query}")

        since_id = SnowFlake.get_tweet_id_from_time(start_time)
        max_id = SnowFlake.get_tweet_id_from_time(end_time)
        logger.info(f"Using boundary tweet ids {since_id} and {max_id}")
        c = 0
        for page in tweepy.Cursor(
            twitter_api.search,
            q=query,
            count=100,
            since_id=since_id,
            max_id=max_id,
            tweet_mode="extended",
        ).pages():
            for status in page:
                extract.retrieve_content_from_tweet(
                    tweet=status._json,
                    db_conn=self._db_conn,
                    include_relations=self._include_relations,
                    include_users=self._include_users,
                )
                c += 1
        logger.info(f"Retrieved {c} tweets")

    def _full_archive_tweets_search(
        self,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        keywords: list = None,
        credentials: dict = None,
        onfly_sampling: bool = True,
        sampling_rate: int = 1,
        bbox: str = None,
        country: str = None,
    ):
        """Use full archive search feature of the v2 Academic API to recover tweets.

        Args:
            onfly_sampling: By default will retrieve all tweets specified in the given
                time period. If False, it will discretise the provided time range in
                seconds-wide blocks to sample 1% of all tweets using time-sampling.

        """

        # if no specified credentials and no env var loaded, query db
        credentials = self._get_academic_credentials(credentials=credentials)
        # here dump all retrieved tweets to output file to be later processed
        raw_output_filename, output_filename = self._get_output_filepaths(
            start_time=start_time, end_time=end_time, keywords_count=len(keywords)
        )

        if not onfly_sampling:
            return self._run_full_archive_continuous_search(
                raw_output_filename=raw_output_filename,
                output_filename=output_filename,
                start_time=start_time,
                end_time=end_time,
                keywords=keywords,
                credentials=credentials,
                bbox=bbox,
                country=country,
            )
        else:
            return self._run_full_archive_discrete_search(
                raw_output_filename=raw_output_filename,
                output_filename=output_filename,
                keywords=keywords,
                start_time=start_time,
                end_time=end_time,
                credentials=credentials,
                sampling_rate=sampling_rate,
                bbox=bbox,
                country=country,
            )

    def _run_full_archive_continuous_search(
        self,
        raw_output_filename: str,
        output_filename: str,
        credentials: dict,
        start_time: datetime,
        end_time: datetime,
        keywords: list = None,
        bbox: str = None,
        country: str = None,
        uid: str = None,
    ):
        """Directly search for all tweets posted in the given timeframes."""

        # retrieve all academic search args to search tweets
        search_args = self._get_academic_search_args(credentials)
        # generate query using searchtweets module
        search_query = self._get_academic_search_query(
            start_time, end_time, keywords=keywords, bbox=bbox, country=country, uid=uid
        )

        return self._execute_full_archive_search(
            search_query=search_query,
            search_args=search_args,
            raw_output_file=raw_output_filename,
            output_file=output_filename,
        )

    def _run_full_archive_discrete_search(
        self,
        raw_output_filename: str,
        output_filename: str,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        credentials: dict,
        keywords: str = None,
        use_tweet_ids: bool = False,
        sampling_rate: int = 1,
        bbox: str = None,
        country: str = None,
    ):
        """Discretize continuous time frame and run full archive search
        on each time block."""

        discrete_time_ranges = self._get_discrete_time_ranges(
            start_time=start_time,
            end_time=end_time,
            use_tweet_ids=use_tweet_ids,
            sampling_factor=sampling_rate,
        )

        # search all tweets for each given interval
        for discrete_time_range in discrete_time_ranges:
            self._run_full_archive_continuous_search(
                raw_output_filename=raw_output_filename,
                output_filename=output_filename,
                keywords=keywords,
                start_time=discrete_time_range[0],
                end_time=discrete_time_range[1],
                credentials=credentials,
                bbox=bbox,
                country=country,
            )

    def _get_discrete_time_ranges(
        self,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        sampling_factor: int = 1,
        sampling_frequency: int = 300,
        use_tweet_ids: bool = False,
    ):
        """Divide continuous time range into chunks.

        The goal of this sampling process is to imitate the Twitter Streaming
            API by randomly fully collecting 1% of the data over discrete
            time periods (at high frequency). Here, with a sampling factor of
            100, we could consider collecting 3 seconds of tweets every 5 minutes.
            Note that we randomly select the specific 3 seconds window within the
            5 minute time range (to avoid random synchronization with exogeneous
            event).

        Args:
            sampling_factor: Amplitude of the sample to be drawn (in percentage).
            sampling_frequency: Frequency of the sample to be drawn from Twitter API.

        """

        # get second size of sample
        sample_size = int(sampling_frequency * sampling_factor / 100)

        # get times in linux epoch ts, compute interval size
        start_ts = start_time.timestamp()
        end_ts = end_time.timestamp()
        sampling_space_size = int(end_ts - start_ts)

        # iterate over
        sampling_intervals = []
        for i in range(0, sampling_space_size, sampling_frequency):
            # determine second offest to get start_ts+i+offset, i.e. 0:0+300-3
            random_interval = random.randint(i, i + sampling_frequency - sample_size)
            tmp_start_ts = start_ts + random_interval
            tmp_end_ts = tmp_start_ts + sample_size

            tmp_start_time = datetime.datetime.fromtimestamp(tmp_start_ts)
            tmp_end_time = datetime.datetime.fromtimestamp(tmp_end_ts)

            if not use_tweet_ids:
                sampling_intervals.append((tmp_start_time, tmp_end_time))
            else:
                tmp_start_tid = SnowFlake.get_tweet_id_from_time(tmp_start_time)
                tmp_end_tid = SnowFlake.get_tweet_id_from_time(tmp_end_time)
                sampling_intervals.append((tmp_start_tid, tmp_end_tid))

        return sampling_intervals

    def _execute_full_archive_search(
        self,
        search_query,
        search_args,
        raw_output_file,
        output_file,
        max_iter=1,
        debug=False,
    ):
        """Execute full archive search using Twitter API V2 endpoint."""

        result_stream = searchtweets.ResultStream(
            request_parameters=search_query,
            max_tweets="no limit",
            **search_args,
        )

        iter_count = 0
        tweet_batch = []
        for response_tweet in result_stream.stream():
            if "next_token" in response_tweet or "newest_id" in response_tweet:
                if debug:
                    print(f"got {len(tweet_batch)} tweets")
                # for traceability, add token element
                tweet_batch.append(response_tweet)
                raw_response_doc = extract._get_raw_response_doc(
                    tweet_batch=tweet_batch
                )

                # load from dict response to Tweet & User objects
                loaded_tweets = convert.Converter.convert_v2_academic_restful_response_to_v1_standard(
                    response_batch=tweet_batch
                )
                # convert all objs to dicts
                loaded_tweet_dicts = [
                    loaded_tweet.to_dict() for loaded_tweet in loaded_tweets
                ]

                # extract & upload the converted tweets to issue db
                for loaded_tweet in loaded_tweet_dicts:
                    extracted_content = extract.retrieve_content_from_tweet(
                        loaded_tweet,
                        db_conn=self.db_conn,
                        include_users=self.include_users,
                        include_relations=self.include_relations,
                    )

                # also upload this full response to the db
                # NOTE: for testing purposes add timestamp as collection tags
                # this_collection_name = db_schema._get_collection_name_from_dt(
                #    start_time=json.loads(search_query)["start_time"],
                #    end_time=json.loads(search_query)["end_time"],
                # )
                self.db_conn.add_restful_response(
                    response=raw_response_doc,
                    # collection_tag=this_collection_name,
                    api_endpoint=versions.ApiVersion.ACADEMIC_V2,
                )

                if settings.STORE_TO_FILE:
                    # save raw tweets + objs
                    loader._save_to_jsonl(
                        content=tweet_batch, output_file=raw_output_file, mode="a"
                    )
                    loader._save_to_jsonl(
                        content=loaded_tweet_dicts, output_file=output_file, mode="a"
                    )

                tweet_batch = []
                iter_count += 1
            else:
                tweet_batch.append(response_tweet)

        return iter_count != 0

    def _get_output_filepaths(
        self, start_time, end_time, keywords_count=None, uid=None
    ):
        """Retrieve output filepaths to store raw responses, and locally filtered responses.
        Note that both files are jsonl files.
        """

        output_ts_str = "%mM%dD%YY_%Hh%Mm"
        start_time_str = datetime.datetime.strftime(start_time, output_ts_str)
        end_time_str = datetime.datetime.strftime(end_time, output_ts_str)
        file_format = ".jsonl"

        tmp_content = keywords_count if keywords_count else uid
        output_filename = f"full_archive_search_{start_time_str}_{end_time_str}_{tmp_content}_tweets{file_format}"
        output_filename = str(settings.OUTPUT_DIR.joinpath(output_filename))

        raw_output_filename = "raw_" + output_filename
        raw_output_filename = str(settings.OUTPUT_DIR.joinpath(raw_output_filename))

        return raw_output_filename, output_filename

    def _get_time_for_twitter_api(self, timestamp):
        """Manually convert the datetime object to the string format required by the Twitter API."""

        return timestamp.strftime(settings.TWITTER_API_V2_TIME_STR)

    def _get_academic_search_query(
        self, start_time, end_time, keywords=None, bbox=None, country=None, uid=None
    ):
        """Get searchtweets query for full archive search using the Academic V2 API."""

        searchtweets_str_format = "%Y-%m-%d %H:%M"
        searchtweets_query = self._get_twitter_api_query(
            keywords=keywords, bbox=bbox, country=country, uid=uid
        )

        # note the dum replace statement, buggy otherwise
        search_query = searching._get_request_parameters(
            query=searchtweets_query,
            start_time=self._get_time_for_twitter_api(start_time),
            end_time=self._get_time_for_twitter_api(end_time),
            tweet_fields=self._get_included_tweet_fields(),
            user_fields=self._get_included_user_fields(),
            expansions=self._get_searchtweets_expansions(),
        )

        return search_query

    def _get_academic_search_args(self, credentials):
        """Get searchtweets args for full archive search using the Academic V2 API."""

        full_archive_endpoint_url = "https://api.twitter.com/2/tweets/search/all"
        yaml_env_searchtweets_filename = ".searchtweets_env.yaml"
        yaml_env_dict = {
            "search_tweets_v2": {
                "endpoint": full_archive_endpoint_url,
                "bearer_token": credentials.bearer_token,
                "consumer_key": credentials.consumer_key,
                "consumer_secret": credentials.consumer_secret,
            }
        }
        yaml_env_filepath = self._create_yaml_env(
            filename=yaml_env_searchtweets_filename, env_dict=yaml_env_dict
        )

        # load credentials with twitter's searchtweets package
        # see https://github.com/twitterdev/search-tweets-python/tree/v2
        search_args = searchtweets.load_credentials(
            filename=str(yaml_env_filepath.absolute()),
            yaml_key="search_tweets_v2",
        )

        return search_args

    def _get_academic_credentials(self, credentials=None):
        """Load Academic API v2 credentials."""

        if not credentials and settings.ACADEMIC_API_BEARER_TOKEN is None:
            pass
        elif not credentials and settings.ACADEMIC_API_BEARER_TOKEN is not None:
            credentials = OmegaConf.create(
                {
                    "bearer_token": settings.ACADEMIC_API_BEARER_TOKEN,
                    "consumer_key": settings.ACADEMIC_API_CONSUMER_KEY,
                    "consumer_secret": settings.ACADEMIC_API_CONSUMER_SECRET,
                }
            )

        if credentials.bearer_token is None:
            raise ValueError(
                "Need to have academic api credentials for full archive search."
            )

        return credentials

    def _get_twitter_api_query(
        self,
        keywords: list = None,
        bbox: str = None,
        country: str = None,
        uid: str = None,
    ):
        """Simple query building to retrieve a tweet containing either of the specified
        keyword. Note that there is a difference between specifying the # character
        or not in front of each keyword."""

        query = ""
        if keywords:
            query += " OR ".join(keywords)
            query = "( " + query + " )"

        if bbox:
            query += f" bounding_box:[{bbox}]"

        if uid:
            query += f" from:{uid}"

        if country:
            # NOTE: requires access to a 2.0 GNIP api
            # query += f" profile_country:{country}"
            query += f" place_country:{country}"

        return query.strip()

    def _get_included_tweet_fields(self):
        """Retrieve list of tweet fields to be included in Twitter API responses. Goal
        is to retrieve tweets with the same amount of data as those returned using the
        tweepy streaming feature."""

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

    def _get_included_user_fields(self):
        """Retrieve list of user fields to be included in Twitter API responses."""

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

    def _get_searchtweets_expansions(self):
        """We need to reconstruct the hashtag, mention, retweet and quote relation
            originating from the published tweet. Thus, we request additional info
            about the user and referenced tweets in the retrieved tweet, to be able
            to process them 'normally' using the extract.retrieve_content_from_tweet
            method.

        Retrieved expansions:
            - author_id:    Retrieves the user object representing the Tweet's author.
            - referenced_tweets.id: Return tweet object referenced by this tweet.
            - in_reply_to_user_id:  Return a user object representing the user this
                requested tweet is a reply of.
            - entities.mentions.username: returns a user obkect for the user mentioned in
                the Tweet.
            - referenced_tweets.id.author_id: returns a user object for the author of the
                reference tweet.
            - geo.place_id: Full place object as mentioned in the original tweet.

        for more info see: https://developer.twitter.com/en/docs/twitter-api/expansions

        """

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

    def search_tweets_with_keywords(
        self,
        keywords: list = None,
        search_field: str = "text",
        search_collection: str = "tweets",
        issue: str = None,
        output_issue: str = None,
    ):
        """Run filter search on database with a given set of keywords. Two methods are available:
        text search in aggregation (therefore directly using db computational resources) or
        iterative search (download tweets and iterate through the whole db locally). We draft a
        method for both and experiment.

        Note that a text index is required on the tweets collection in order to query it. Ran creation
            of text index on the 24 million tweets database (might be too much).
        """

        tmp_issue = self._get_issue(issue=issue)
        output_issue = tmp_issue if not output_issue else output_issue
        return searching.run_local_tweets_search(
            keywords=keywords,
            search_field=search_field,
            search_collection=search_collection,
            issue=tmp_issue,
            output_issue=output_issue,
            db_conn=self.db_conn,
        )

    def _create_yaml_env(self, filename, env_dict):
        """Transform dict to yaml env save in current root directory."""

        with open(filename, "w") as f:
            yaml.dump(env_dict, f)

        return settings.SNPIPELINE_HOME.joinpath(filename)

    def _is_valid_timestamp(self, timestamp):
        """Check if this timestamp goes back more than 7 days from current date."""

        rn_timestamp = datetime.datetime.now()
        seven_days_delta = datetime.timedelta(days=7)

        return (timestamp - rn_timestamp) <= seven_days_delta

    def fetch_users_timelines(
        self, usernames=None, uids=None, local_credentials=True, api_credentials=None
    ):
        # get a twitter api, optionally using the allowed credentials
        if local_credentials and not api_credentials:
            kwargs = dict(
                credentials=dict(
                    access_token=settings.ACCESS_TOKEN,
                    access_token_secret=settings.ACCESS_TOKEN_SECRET,
                    consumer_key=settings.CONSUMER_KEY,
                    consumer_secret=settings.CONSUMER_SECRET,
                )
            )
        elif api_credentials:
            assert (
                "access_token" in api_credentials
                and "access_token_secret" in api_credentials
                and "consumer_key" in api_credentials
                and "consumer_secret" in api_credentials
            )
            kwargs = dict(credentials=api_credentials)
        else:
            kwargs = dict(db_conn=self._db_conn)
        twitter_api = credentials._get_twitter_api(**kwargs)
        print(
            f"Retrived twitter api: {twitter_api} with local credentials ({local_credentials})"
        )

        if usernames:
            print("Starting to fetch timelines for {} usernames".format(len(usernames)))
            for idx, username in enumerate(usernames):
                history._get_user_history(
                    username=username,
                    twitter_api=twitter_api,
                    db_connection=self._db_conn,
                )
                if idx % 1000 == 0 and idx != 0:
                    print(f"Fetched {idx+1} timelines")

        elif uids:
            print("Starting to fetch timelines for {} uids".format(len(uids)))
            for idx, uid in enumerate(uids):
                history._get_user_history(
                    uid=uid, twitter_api=twitter_api, db_connection=self._db_conn
                )

                if idx % 1000 == 0 and idx != 0:
                    print(f"Fetched {idx+1} timelines")
