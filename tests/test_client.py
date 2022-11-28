import json

import searchtweets
import pytest

from tweepipe import settings
from tweepipe.legacy import client
from tweepipe.legacy.utils import extract
from tweepipe.utils import loader
from tweepipe.utils.migration import mapping, tweet, convert

from tests.utils import test_config, mockmongo


class ClientTest(mockmongo.MockMongo):
    def setUp(
        self,
    ):
        super().setUp()

    @pytest.mark.skip
    def test_get_academic_credentials_from_config(self):
        """Verify retrieval of tweets using the Twitter v2 API."""

        snclient = client.Client()
        credentials = snclient._get_academic_credentials()

        self.assertTrue(credentials.bearer_token == settings.ACADEMIC_API_BEARER_TOKEN)
        self.assertTrue(credentials.consumer_key == settings.ACADEMIC_API_CONSUMER_KEY)
        self.assertTrue(
            credentials.consumer_secret == settings.ACADEMIC_API_CONSUMER_SECRET
        )

    @pytest.mark.skip
    def test_academic_search_tweets(self):
        """Check that the client effectively retrieves tweets."""

        snclient = client.Client()
        raw_file, output_file = snclient._get_output_filepaths(
            test_config.start_time_ts,
            test_config.end_time_ts,
            len(test_config.keywords),
        )

        self.assertTrue(raw_file == test_config.full_archive_raw_output_filename)
        self.assertTrue(output_file == test_config.full_archive_output_filename)

    @pytest.mark.skip
    def test_academic_search_args(
        self,
    ):
        """Verify the arguments produced by the clients for a v2 academic search."""

        snclient = client.Client()
        credentials = snclient._get_academic_credentials()
        searchargs = snclient._get_academic_search_args(credentials)

        self.assertTrue(searchargs == test_config.searchargs)

    @pytest.mark.skip
    def test_tweet_models(self):
        """Test Tweet models defined in migration folder."""

        tweets = convert.Converter.convert_restful_response(
            response_batch=test_config.sample_v2_response[:11],
        )

        tweet_dict = tweets[0].to_dict()
        extracted_content = extract.retrieve_content_from_tweet(tweet_dict)

        # check relations have been created
        self.assertTrue(("hashtag" in extracted_content["relations"]))
        self.assertTrue(("retweet" in extracted_content["relations"]))
        self.assertTrue(("mention" in extracted_content["relations"]))
        self.assertTrue(("quote" in extracted_content["relations"]))

    def test_recover_tweets(self):
        snclient = client.Client()
        settings.load_academic_api_credentials(env_file=".env")

        snclient.recover_tweets(
            keywords=test_config.keywords,
            start_time=test_config.start_time_ts,
            end_time=test_config.end_time_ts,
        )

    @pytest.mark.skip
    def test_academic_search_query(self):
        """Check that the query is well created for v2 search tweets.

        Note that this test requires to have a .env file with academic
            API at the root of the project directory to run the specified
            academic api queries.

        """

        snclient = client.Client()
        settings.load_academic_api_credentials(env_file=".env")

        credentials = snclient._get_academic_credentials()
        search_args = snclient._get_academic_search_args(credentials)
        search_query = snclient._get_academic_search_query(
            test_config.keywords,
            test_config.start_time_ts,
            test_config.end_time_ts,
        )

        snclient._execute_full_archive_search(
            search_query=search_query,
            search_args=search_args,
            raw_output_file=str(
                settings.OUTPUT_DIR.joinpath(
                    test_config.full_archive_raw_output_filename
                )
            ),
            output_file=str(
                settings.OUTPUT_DIR.joinpath(test_config.full_archive_output_filename)
            ),
            max_results=test_config.max_results,
            max_iter=test_config.max_iter,
        )

        # load output files and check that the result
        downloaded_tweets = loader._load_from_file(
            test_config.full_archive_output_filename
        )
        self.assertTrue(
            len(downloaded_tweets) == (test_config.max_results * test_config.max_iter)
        )
