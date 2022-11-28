import os
import unittest

from tweepipe.db import db_client
from tweepipe import settings

from tests.utils import test_config


class MockMongo(unittest.TestCase):
    def setUp(
        self,
    ):
        """Create database based on test .env file."""

        settings.load_config(env_file=test_config.test_env_file)
        self.clean_working_files()

        # create db client
        self.db_connection = db_client.DBClient(issue=test_config.test_db_issue)

        # make sure connected locally, to the test database
        assert settings.MONGO_HOST == "127.0.0.1"
        assert settings.MONGO_PORT == 27017
        assert settings.MONGO_DB == test_config.test_db_issue

        # drop previous collections
        self.db_connection.clear_database(
            db_name=test_config.test_db_issue, are_you_sure=True
        )
        self.db_connection.init_collections(self.db_connection._db_schema)

    def tearDown(self):
        """Destroy test database."""

        self.clean_working_files()
        # self.db_connection.clear_database(db_name=test_config.test_db_issue, are_you_sure=True)

    def clean_working_files(self):
        """Remove test files for full archive academic search."""

        raw_output_file = settings.OUTPUT_DIR.joinpath(
            test_config.full_archive_raw_output_filename
        )
        if raw_output_file.is_file():
            os.remove(str(raw_output_file))

        output_file = settings.OUTPUT_DIR.joinpath(
            test_config.full_archive_output_filename
        )
        if output_file.is_file():
            os.remove(str(output_file))
