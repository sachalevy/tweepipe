import json
import math
import time
import threading
import queue
from tweepipe.legacy.utils import extract

import tweepy
from loguru import logger

from tweepipe.db import db_client
from tweepipe.utils import tracker, credentials


def stream_keywords(
    twitter_credentials: dict,
    keywords: list = [],
    issue: str = "stream",
    include_relations: bool = True,
    include_users: bool = True,
    output_file: str = None,
    skip_db: bool = False,
):

    stream_log_file = f"seq_stream_log_{issue}.log"
    logger.add(stream_log_file)
    logger.info(f"Starting streaming for {issue}, saving at {stream_log_file}.")

    try:
        if not skip_db:
            db_conn = db_client.DBClient(
                issue=issue,
                include_relations=include_relations,
                include_users=include_users,
            )
        else:
            db_conn = None
    except Exception:
        db_conn = None
        pass

    # implement exponential backoff in case of trouble
    exp_backoff, backoff_param = 1, 1.4
    while True:
        try:
            # reset the exponential backoff when going beyond 15 minutes
            if exp_backoff > 900:
                exp_backoff = 1.4

            print("credentials", twitter_credentials)
            twitter_auth = credentials._get_twitter_auth(twitter_credentials)
            stream_listener = SNPipelineStream(
                db_conn=db_conn,
                output_file=output_file,
                include_users=include_users,
                include_relations=include_relations,
            )
            stream = tweepy.Stream(auth=twitter_auth, listener=stream_listener)
            stream.filter(track=keywords, stall_warnings=True)
        except Exception as e:
            error_msg = f"Error while streaming with {len(keywords)} keywords., waiting {exp_backoff} seconds..."
            tracker.log_error(e, msg=error_msg)
            logger.error(error_msg)
            time.sleep(exp_backoff)

            # backing off with an exponential factor of 1.4
            exp_backoff = exp_backoff * backoff_param


def _run_streamer(
    twitter_auth: dict,
    keywords: list = [],
    issue: str = "stream",
    include_relations: bool = True,
    include_users: bool = True,
):
    """Helper function dedicated to running streaming processes in parallel."""
    stream_log_file = f"stream_log_{issue}.log"
    logger.add(stream_log_file)
    logger.info(f"Starting streaming for {issue}, saving at {stream_log_file}.")

    db_conn = db_client.DBClient(
        issue=issue, include_relations=include_relations, include_users=include_users
    )
    stream_listener = SNPipelineStream(db_conn=db_conn)
    stream = tweepy.Stream(auth=twitter_auth, listener=stream_listener)
    stream.filter(track=keywords, stall_warnings=True)


def _get_parallel_streaming_kwargs(
    keywords: list,
    twitter_auths: list,
    process_count: int,
    issue: str,
    include_users: bool = True,
    include_relations: bool = True,
):
    """Retrieve a list of keyword args for streaming keywords in parallel. These
    include twitter_auth accesses, the list of keywords to stream as well as
    the number of processes to use and type of information to retrieve.
    """

    batch_size = math.ceil(len(keywords) / process_count)
    keywords_batches = [
        keywords[i : i + batch_size] for i in range(0, len(keywords), batch_size)
    ]

    kwargs_list = []
    for keyword_batch, twitter_auth in zip(keywords_batches, twitter_auths):
        tmp_kwargs = dict(
            twitter_auth=twitter_auth,
            keywords=keyword_batch,
            issue=issue,
            include_users=include_users,
            include_relations=include_relations,
        )
        kwargs_list.append(tmp_kwargs)

    return kwargs_list


class SNPipelineStream(tweepy.StreamListener):
    def __init__(
        self,
        db_conn: db_client.DBClient = None,
        output_file: str = None,
        include_users: bool = True,
        include_relations: bool = True,
    ):
        super(SNPipelineStream, self).__init__()

        self.db_conn = db_conn
        self.output_file = output_file
        self.include_users = include_users
        self.include_relations = include_relations
        self.processing_queue = queue.Queue()
        self.processing_thread = threading.Thread(
            target=self.process_incoming_tweets, daemon=True
        )
        self.processing_thread.start()

    def process_incoming_tweets(self, heartbeat: float = 1):
        while True:
            if self.processing_queue.empty():
                # logger.debug(f"No tweet, waiting {heartbeat} sec.")
                time.sleep(heartbeat)
                continue
            else:
                try:
                    tweet = self.processing_queue.get(block=False)
                    # logger.debug(
                    #    f'Processing tweet {tweet.get("id")} - queue size {self.processing_queue.qsize()} -batch size {len(self.db_conn.tweet_batch)}.'
                    # )
                    extract.retrieve_content_from_tweet(
                        tweet=tweet,
                        db_conn=self.db_conn,
                        include_users=self.include_users,
                        include_relations=self.include_relations,
                        output_file=self.output_file,
                    )
                    self.processing_queue.task_done()
                except queue.Empty as e:
                    logger.error(e)
                    continue

    def on_status(self, status):
        """Add loaded tweet status to the queue of incoming tweet to be
        processed."""
        try:
            # logger.debug(f"Adding tweet {status.id_str} to processing queue.")
            self.processing_queue.put(status._json)
        except queue.Full as e:
            logger.error(f"Error: {e}, queue size: {self.processing_queue.qsize()}.")
            raise e

        return True

    def keep_alive(self):
        """Called when a keep-alive arrived"""
        return

    def dump_processing_queue_content(self):
        tweets = []
        while not self.processing_queue.empty():
            try:
                tweets.append(self.processing_queue.get_nowait())
            except queue.Empty as e:
                logger.error(e)

        with open("emergency_tweet_queue_dump.json", "w") as f:
            json.dump(tweets, f)

    def on_exception(self, exception):
        """Called when an unhandled exception occurs."""
        # TODO: check if should perform following operation
        try:
            self.db_conn.flush_content()
        except Exception as e:
            pass
        self.dump_processing_queue_content()
        logger.error(exception)
        raise Exception(exception)

    # TODO: implement sleeping so avoid getting
    def on_limit(self, track):
        """Called when a limitation notice arrives"""
        try:
            self.db_conn.flush_content()
        except Exception as e:
            pass
        error_msg = f"Reached credential limit {track}."
        logger.warning(error_msg)
        raise TimeoutError(error_msg)

    def on_error(self, status_code):
        """Called when a non-200 status code is returned"""
        try:
            self.db_conn.flush_content()
        except Exception as e:
            pass
        error_msg = f"Received error on Twitter stream with code: {status_code}."
        logger.error(error_msg)
        raise Exception(error_msg)

    def on_timeout(self):
        """Called when stream connection times out"""
        try:
            self.db.flush_content()
        except Exception as e:
            pass
        timeout_msg = "Twitter connection timed out."
        logger.error(timeout_msg)
        raise TimeoutError(timeout_msg)

    def on_disconnect(self, notice):
        """Called when twitter sends a disconnect notice
        Disconnect codes are listed here:
        https://developer.twitter.com/en/docs/tweets/filter-realtime/guides/streaming-message-types
        """
        try:
            self.db_conn.flush_content()
        except Exception as e:
            pass
        disconnect_msg = f"Twitter client disconnecting with notice: {notice}."
        logger.error(disconnect_msg)
        raise Exception(disconnect_msg)

    def on_warning(self, notice):
        """Called when a disconnection warning message arrives"""
        self.db_conn.flush_content()
        warning_msg = f"Warning on Twitter Stream: {notice}."
        logger.warning(warning_msg)


# future snpipeline stream module when tweepy 4.0 releases
class SNPipelineStreamFuture(tweepy.Stream):
    def __init__(
        self,
        credentials: dict,
        db_conn: db_client.DBClient = None,
        output_file: str = None,
    ):
        # TODO: check if super init valid
        super(SNPipelineStreamFuture, self).__init__(**credentials)
        self.db_conn = db_conn
        self.output_file = output_file

    def on_data(self, data: str):
        tweet = json.loads(data)

        if "id_str" not in tweet:
            return True

        extract.retrieve_content_from_tweet(
            tweet,
            db_conn=self.db_conn,
            output_file=self.output_file,
            include_users=self.db_conn.include_users,
            include_relations=self.db_conn.include_relations,
        )

        return True
