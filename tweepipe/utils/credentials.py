from pathlib import Path
import json

import tweepy
from tweepy import OAuthHandler
from loguru import logger

from tweepipe.db import db_client

TWITTER_API_PURPOSES = ["any", "lookup", "search", "stream", "hydrate"]


def fetch_issue_credentials(issue):
    filepath = Path(f".{issue}.json")
    if not filepath.is_file():
        raise FileNotFoundError(f"Cannot find credentials file for issue {issue}.")

    with open(filepath) as f:
        credentials = json.load(f)

    return credentials


def free_api(db_conn: db_client.DBClient, twitter_credentials: list, purpose: str):
    """Set the API status to 0 post collection."""
    for twitter_credential in twitter_credentials:
        db_conn.update_twitter_credentials(
            consumer_key=twitter_credential.get("consumer_key"),
            purpose=purpose,
        )


def _get_twitter_credentials(db_conn, api_count=-1, purpose="any", free_api=False):
    """Retrieve N twitter credentials from db."""

    if purpose not in TWITTER_API_PURPOSES:
        raise ValueError(
            "Invalid purpose. Choose among the following: {}.".format(
                TWITTER_API_PURPOSES
            )
        )

    return db_conn.get_twitter_credentials(
        count=api_count, purpose=purpose, must_be_free=free_api
    )


# TODO: check if instantaniation of twitter credentials valid
def _get_twitter_auth(credentials):
    twitter_auth = OAuthHandler(
        credentials["consumer_key"], credentials["consumer_secret"]
    )
    if "access_token" in credentials and "access_token_secret" in credentials:
        twitter_auth.set_access_token(
            credentials["access_token"], credentials["access_token_secret"]
        )

    return twitter_auth


def _get_twitter_auths(credentials):
    """Retrieve multiple twitter auths object."""

    twitter_auths = [_get_twitter_auth(cred_pair) for cred_pair in credentials]

    return twitter_auths


def _get_twitter_api(
    db_conn: db_client.DBClient = None, purpose: str = "", credentials: dict = {}
):
    """Get a single twitter api."""

    if not credentials and db_conn:
        credentials = _get_twitter_credentials(db_conn, api_count=1, purpose=purpose)[0]
    elif not credentials:
        raise RuntimeError("No credentials provided.")

    twitter_auth = _get_twitter_auth(credentials)
    twitter_api = tweepy.API(
        twitter_auth, wait_on_rate_limit=True, retry_count=10, retry_delay=3
    )

    return twitter_api


def _get_twitter_apis(
    db_conn: db_client.DBClient = None,
    purpose: str = "",
    api_count=-1,
    twitter_credentials: list = [],
    free_api: bool = False,
):
    """Get multiple twitter apis."""

    if not twitter_credentials and db_conn:
        twitter_credentials = _get_twitter_credentials(
            db_conn, api_count=api_count, purpose=purpose, free_api=free_api
        )
    elif not twitter_credentials:
        raise RuntimeError("No credentials provided.")

    logger.info(f"Retrieving {len(twitter_credentials)}")

    twitter_auths = _get_twitter_auths(twitter_credentials)

    twitter_apis = []
    for twitter_auth in twitter_auths:
        tmp_twitter_api = tweepy.API(
            twitter_auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True
        )
        twitter_apis.append(tmp_twitter_api)

    return twitter_apis, twitter_credentials
