import pymongo
from pymongo import MongoClient
import bson
from typing import Any, Union
import time
import json

from loguru import logger

from tweepipe import settings
from tweepipe.db import db_schema
from tweepipe.utils import errors


class DBClient:
    """A simple and functional wrapper around the pymongo client to work with Twitter data
    in the JSON format. This client includes utilities to manage Twitter API credentials
    stored in a mongo database, as well as manage queues of data to be hydrated, fetched,
    or processed.

    :param issue: Name of the current working database, defaults to None.
    :type issue: str, optional
    :param response_mirror_name: Mirror collection to store raw API v2
        responses in addition to the tweets, defaults to None.
    :type response_mirror_name: str, optional
    :param include_relations: Whether to extract relations from tweets, defaults to True.
    :type include_relations: bool, optional
    :param include_users: Whether to extract relations from users, defaults to True.
    :type include_users: bool, optional
    :param batch_size: Batch size for bulk data lists, defaults to 128.
    :type batch_size: int, optional
    :param schema: Index schema for the working database, defaults to db_schema.DEFAULT_TWITTER_DB_SCHEMA.
    :type schema: dict, optional
    """

    def __init__(
        self,
        issue: str = None,
        response_mirror_name: str = None,
        include_relations: bool = True,
        include_users: bool = True,
        batch_size: int = 1024,
        schema: dict = None,
        declared_collections: list = None,
        block_index_create: bool = False,
    ):
        self.conn = MongoClient(
            settings.MONGO_HOST,
            settings.MONGO_PORT,
            username=settings.MONGO_USERNAME,
            password=settings.MONGO_PASSWORD,
            ssl=True,
        )
        self.block_index_create = block_index_create

        # determine db name
        self.declared_collections = declared_collections
        issue = self._get_db_name(db_name=issue)
        self.issue = issue if issue != None else "default"

        self.response_mirror_name = (
            response_mirror_name
            if response_mirror_name != None
            else settings.RESPONSE_MIRROR_COLLECTION_NAME
        )

        self.include_users = include_users
        self.include_relations = include_relations
        self.batch_size = batch_size
        self.bulk_data = {}

        self._db_schema = db_schema._get_db_schema(
            issue=issue,
            include_users=include_users,
            include_relations=include_relations,
            schema_base=schema if schema else db_schema.DEFAULT_TWITTER_DB_SCHEMA,
        )
        self.init_collections(_db_schema=self._db_schema)
        self.init_bulk_buffers()

    def init_bulk_buffers(self):
        """Create temporary buffers to push data in bulk to the database. Initialize
        buffers after the employed schema. If the user is trying to push data not
        available with the current schema, addition and push operations should fail.
        """

        if self.declared_collections:
            for collection_name in self.declared_collections:
                self.bulk_data[collection_name] = []

        for collection_name in self._db_schema.get(self.issue):
            self.bulk_data[collection_name] = []
        # logger.debug(f"Initialized bulk buffers for {', '.join(list(self.bulk_data))}")

    def init_collections(self, _db_schema: dict):
        """Re-create collections in MongoDB server using provided indexes."""

        for db_name in _db_schema:
            for collection_name in _db_schema[db_name]:
                # logger.info(f"Creating collection {collection_name} in db {db_name}")
                collection_conn = self.conn[db_name][collection_name]
                for index in _db_schema[db_name][collection_name]:
                    try:
                        if not self.block_index_create:
                            collection_conn.create_index(
                                index["index"],
                                unique=index.get("unique", False),
                            )
                    except pymongo.errors.OperationFailure as e:
                        pass

    def _get_collection(self, collection_name: str, db_name: str = None):
        """Retrieve connection to the specified database collection."""

        db_name = self.issue if not db_name else db_name

        return self.conn[db_name][collection_name]

    def clear_database(self, db_name: str = None, are_you_sure: bool = False):
        """Remove all collections and data from the specified database."""

        if not are_you_sure:
            raise ValueError("You must be sure before deleting a database!")

        db_name = self._get_db_name(db_name)

        if (
            self.conn.address != ("127.0.0.1", 27017)
            and self.conn.address
            != (
                "localhost",
                27017,
            )
            and not are_you_sure
        ):
            raise PermissionError(
                "Are you sure you want to delete a non-local database?"
            )

        try:
            self.conn.drop_database(db_name)
        except pymongo.errors.OperationFailure as e:
            pass

    def _load_bson_object_ids(self, batch: Union[list, set]) -> list:
        """Load string bson ids to Bson Python objects."""

        for doc in batch:
            doc["_id"] = bson.objectid.ObjectId(doc["_id"]["$oid"])

        return batch

    def _get_db_name(self, db_name: str = None):
        """Returns db_name if no settings specified, else the predefined name."""

        db_name = db_name if db_name is not None else settings.MONGO_DB

        return db_name

    def add_restful_response(
        self, response: list, collection_tag: str = None, api_endpoint: str = None
    ):
        """Add Academic/Standard api response to db."""

        # make sure restful response mirror of collection exists
        response_collection = self._check_create_response_mirror(
            collection_tag=collection_tag
        )

        # insert response document into collection
        response_collection.insert_one(response)

    def _check_create_response_mirror(
        self, collection_tag: str = None, api_endpoint: str = None
    ):
        """Checks whether or not raw response collections are available for
        the curent database. If not, init a new one.

        Args:
            collection_tag: Collection name to search for in the current db.

        Returns:
            Returns True if a new collection was created. Otherwise, returns False.
        """

        response_mirror_name = self._get_response_mirror_name(
            collection_tag=collection_tag, api_endpoint=api_endpoint
        )

        return self._get_collection(collection_name=response_mirror_name)

    def _get_response_mirror_name(
        self, collection_tag: str = None, api_endpoint: str = None
    ):
        """Checks for response mirror tag, otherwise retrieves default settings value."""

        collection_name = (
            collection_tag if collection_tag != None else self.response_mirror_name
        )
        if api_endpoint:
            collection_name += "".join(["_", api_endpoint])

        return collection_name

    def add_to_collection(self, collection_name: str, data: Any):
        """Add json data to its collection's buffer before pushing it to the database.

        :param collection_name: name of the collection to add data to
        :type collection_name: str
        :param data: data to be added
        :type data: Any
        :raises ValueError: only add data to collections available in working db
        """
        if collection_name not in self.bulk_data:
            raise ValueError("Undefined collection for current schema.")

        if isinstance(data, list):
            self.bulk_data[collection_name].extend(data)
        else:
            self.bulk_data[collection_name].append(data)

        if len(self.bulk_data[collection_name]) >= self.batch_size:
            self._push_bulk_data(collection_name)

    def add_tweet(self, tweet: dict):
        self.add_to_collection("tweets", tweet)

    def add_user(self, user: dict):
        self.add_to_collection("users", user)

    def add_like(self, like: dict):
        self.add_to_collection("likes", like)

    def add_media(self, media: Union[dict, list]):
        self.add_to_collection("media", media)

    def add_poll(self, poll: Union[dict, list]):
        self.add_to_collection("polls", poll)

    def add_place(self, place: Union[dict, list]):
        self.add_to_collection("places", place)

    def add_users(self, users: list):
        self.add_to_collection("users", users)

    def add_relation_extraction(self, relation_extraction: dict):
        self.add_to_collection("relation_extraction", relation_extraction)

    def add_fetching_uids(self, uids: list):
        self.add_to_collection("fetching_uids", uids)

    def add_missing_tids(self, tids: list):
        self.add_to_collection("missing_tids", tids)

    def add_hydrating_tids(self, tids: list):
        self.add_to_collection("hydrating_tids", tids)

    def add_missing_users(self, missing_uids: list):
        self.add_to_collection("missing_users", missing_uids)

    def add_hashtags(self, hashtags: list):
        self.add_to_collection("hashtags", hashtags)

    def add_retweets(self, retweets: list):
        self.add_to_collection("retweets", retweets)

    def add_mentions(self, mentions: list):
        self.add_to_collection("mentions", mentions)

    def add_quotes(self, quotes: list):
        self.add_to_collection("quotes", quotes)

    def add_replies(self, replies: list):
        self.add_to_collection("replies", replies)

    def add_scores(self, scores: list):
        self.add_to_collection("users", scores)

    def get_collection_paginator(
        self, collection_name: str, projection: dict = None, page_size: int = 2048
    ) -> pymongo.cursor.Cursor:

        collection = self._get_collection(collection_name)
        cursor = collection.find(
            projection=projection,
            batch_size=page_size,
            sort=[("_id", pymongo.ASCENDING)],
            allow_disk_use=True,
        )

        return cursor

    def create_hydrating_index(self):
        """Enable fast query on status field for hydrating tweets."""

        hydrating_tids_collection = self._get_collection(
            collection_name="hydrating_tids",
            db_name=self.issue,
        )
        hydrating_tids_collection.create_index(
            "status",
        )
        hydrating_tids_collection.create_index("tid", unique=True)

    def _push_bulk_data(self, collection_name: str):
        """Push a batch of data to the collection on the working database.

        :param collection_name: Collection name to push data to.
        :type collection_name: str
        """
        collection = self._get_collection(collection_name, db_name=self.issue)
        logger.info(f"Pushing to collection {collection_name} in db {self.issue}")
        try:
            logger.info(
                f"pushing {len(self.bulk_data[collection_name])} elements to {collection_name}"
            )
            if collection_name == "users":
                logger.info(
                    f"Individual user ids: {len(set([user.get('uid') for user in self.bulk_data[collection_name]]))}"
                )
                logger.info(
                    f"Number of records to insert: {len(self.bulk_data[collection_name])}"
                )
            # print(json.dumps(self.bulk_data[collection_name][0], indent=2))
            # time.sleep(100)
            collection.insert_many(self.bulk_data[collection_name], ordered=False)
        except pymongo.errors.BulkWriteError as e:
            print(f"Encountered error while pushing to {collection_name}")  # , e)
            pass

        # reset collection bulk buffer
        self.bulk_data[collection_name] = []

    def flush_content(self):
        """Flush cached tweets to db upon exit."""

        for collection_name in self.bulk_data:
            if len(self.bulk_data.get(collection_name)) > 0:
                self._push_bulk_data(collection_name)

    def reset_twitter_credentials_statuses(self):
        """Resets Twitter credentials use statuses."""

        api_collection = self._get_collection("api", db="api")

        return api_collection.update_many(
            filter={},
            update={"$set": {"lookup": 0, "search": 0, "stream": 0, "any": 0}},
        )

    def insert_twitter_credentials(self, twitter_credentials: list):
        """Insert Twitter API credentials into dataabse.

        Args:
            twitter_credentials: Dict-like object containing Twitter API
                credentials to add to the database. A Twitter API credential
                should contain the following fields: "access_token",
                "access_token_secret", "consumer_key", "consumer_secret".

        """

        api_collection = self._get_collection(collection_name="api", db_name="api")
        try:
            api_collection.insert_many(twitter_credentials, ordered=False)
        except pymongo.errors.BulkWriteError as e:
            pass

    def get(self, collection_name: str, params: dict = None):
        """List all documents in a collection."""
        collection = self._get_collection(collection_name, db_name=self.issue)
        return collection.find(filter=params)

    def get_twitter_credentials(self, count=1, purpose="any", must_be_free=False):
        """Retrieve Twitter API credentials.

        Args:
            credentials_count: Number of Twitter API credentials to retrieve.
            purpose: Use case motivating the retrieval. Useful for resource
                allocation.

        Returns:
            A list of retrieved apis.

        """

        api_collection = self._get_collection(collection_name="api", db_name="api")

        count = 0 if count == -1 else count
        params = dict(limit=count)

        # avoid having any busy api
        params["filter"] = {}
        if must_be_free:
            _filter = {purpose: 0}
            params["filter"].update(_filter)

        params["projection"] = dict(
            access_token=1, access_token_secret=1, consumer_key=1, consumer_secret=1
        )

        # retrieve free Twitter API credentials
        retrieved_credentials = list(api_collection.find(**params))

        # update their status in db
        bulk_updates = []
        for credential in retrieved_credentials:
            bulk_updates.append(
                pymongo.UpdateOne(
                    {"access_token": credential["access_token"]}, {"$set": {purpose: 1}}
                )
            )
        if bulk_updates:
            api_collection.bulk_write(bulk_updates)

        # raise an error if could not retrieve the right amount of credentials
        if len(retrieved_credentials) < count:
            raise errors.SnPipelineError(
                errors.SnPipelineErrorMsg.UNSUFFICIENT_FREE_RESOURCES
            )

        return retrieved_credentials

    def update_twitter_credentials(
        self, consumer_key: str, purpose: str, status: int = 0
    ):
        """Update the consumer key utilization status."""

        query = {"consumer_key": consumer_key}
        update = {"$set": {f"{purpose}": status}}
        api_collection = self._get_collection(collection_name="api", db_name="api")
        api_collection.update_one(query, update)
