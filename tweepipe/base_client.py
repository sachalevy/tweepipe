from tweepipe import settings
from tweepipe.db import db_client, db_schema


class BaseClient:
    """
    Tweepipe data collection client.

    The tweepipe client wraps around the tweepy client to facilitate data storage
    to MongoDB.

    :param issue: Name of the ongoing issue, also the working database. Defaults to None.
    :type issue: str, optional
    :param include_users: Whether to extract user profiles upon processing the retrieved tweets.
        If so, the users will be stored in a separate collection. Defaults to True.
    :type include_users: bool, optional
    :param include_relations: Whether to extract relations (hashtags, quotes, mentions, retweets)
        upon processing. Like users, if extracted they will be stored under separate collections
        in the mongo database. Defaults to True.
    :type include_relations: bool, optional
    :param skip_db: Set to True to skip usage of a database, and not have persistent storage on
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
    ):

        self._issue = issue
        self._include_users = include_users
        self._include_relations = include_relations
        self._schema = schema if schema else db_schema.INDEX_V3

        if skip_db:
            self._db_conn = None
        else:
            self._db_conn = db_client.DBClient(
                issue=issue,
                include_users=include_users,
                include_relations=include_relations,
                schema=schema,
            )

        self._env_file = None

    def _get_issue(self, issue: str = None) -> str:
        """Determine the current working issue.

        :param issue: specified working issue, defaults to None.
        :type issue: str, optional
        :return: Current working issue.
        :rtype: str
        """

        tmp_issue = issue if issue else self.issue

        return tmp_issue

    def _get_include_users(self, include_users: bool = False) -> bool:
        """
        Determine whether to include users, if either the client's flag is set,
        or this flag was set.


        :param include_users: include users flag, defaults to False.
        :type include_users: bool, optional
        :return: final include users flag
        :rtype: bool
        """

        return include_users or self.include_users

    def _get_include_relations(self, include_relations: bool = False) -> bool:
        """
        Determine whether to include relations, if either the client's flag is set,
        or this flag was set.

        :param include_relations: include relations flag, defaults to False.
        :type include_relations: bool, optional
        :return: final include relations flag
        :rtype: bool
        """

        return include_relations or self.include_relations

    def set_issue(self, new_issue: str):
        """Change name of working issue, and refresh db connection.

        :param new_issue: New issue name
        :type new_issue: str
        """

        if new_issue != self.issue:
            self._issue = new_issue
            self._db_conn = db_client.DBClient(
                issue=new_issue,
                include_users=self.include_users,
                include_relations=self.include_relations,
                schema=self.schema,
            )

    def set_mirror_collection_name(self, mirror_collection_name: str):
        """Set name of collection handling the storage of raw api response.

        :param mirror_collection_name: mirror collection name
        :type mirror_collection_name: str
        """

        self._db_conn.response_mirror_name = mirror_collection_name

    def set_include_users(self, include_users: bool = True):
        self._include_users = include_users

    def set_include_relations(self, include_relations: bool = True):
        self._include_relations = include_relations

    def set_env_file(self, env_file: str):
        """Update environment file path, and load it in settings.

        :param env_file: filepath to environment file
        :type env_file: str
        """

        self._env_file = env_file
        settings.load_config(env_file=self.env_file)

    @property
    def db_conn(self) -> db_client.DBClient:
        """Get current database connection.

        :return: Current client database connection.
        :rtype: db_client.DBClient
        """
        return self._db_conn

    @property
    def schema(self) -> dict:
        """Get current database index schema.

        :return: current working database index schema
        :rtype: dict
        """
        return self._schema

    @property
    def include_users(self) -> bool:
        """Get include users flag.

        :return: include users flag
        :rtype: bool
        """
        return self._include_users

    @property
    def include_relations(self) -> bool:
        """Get include relations flag.

        :return: include relations flag
        :rtype: bool
        """
        return self._include_relations

    @property
    def issue(self) -> str:
        """Get working subject, also working database name.

        :return: issue name
        :rtype: str
        """
        return self._issue

    @property
    def env_file(self) -> str:
        """Get environment file path.

        :return: env file path
        :rtype: str
        """
        return self._env_file
