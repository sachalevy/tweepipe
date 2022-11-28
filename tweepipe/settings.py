import os
from multiprocessing import cpu_count
from pathlib import Path

from dotenv import load_dotenv

import tweepipe

MONGO_DB = None
MONGO_USERNAME = None
MONGO_PASSWORD = None
MONGO_HOST = None
MONGO_PORT = None

RESPONSE_MIRROR_COLLECTION_NAME = "raw_response"

CPU_COUNT = cpu_count() - 2
WORKER_COUNT = 8

STORE_TO_FILE = False

CHROME_DRIVER_EXE = None

SLACK_BOT_TOKEN = None
DATA_COLLECTION_CHANNEL_ID = None

CURRENT_TASK = None

ACADEMIC_API_BEARER_TOKEN = None
ACADEMIC_API_CONSUMER_KEY = None
ACADEMIC_API_CONSUMER_SECRET = None

SNPIPELINE_HOME = Path.cwd()
OUTPUT_DIR = SNPIPELINE_HOME
BOTSPOT_MODEL = Path("tweepipe/legacy/botspot/model/botspot_file.pkl")

TWEET_TS_STR_FORMAT = "%a %b %d %H:%M:%S +0000 %Y"
TWITTER_API_V2_TIME_STR = TWITTER_API_V1_STR_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
TWITTER_API_ACADEMIC_V2_STR_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def set_current_task(task: str = None):
    """
    Define the current task of the pipeline.

    Args:
        task (str, optional): Task name. Defaults to None.
    """
    tweepipe.settings.CURRENT_TASK = task


def load_config(env_file: str = None):
    """
    Set tweepipe global variables according to dotenv environment variables.

    Args:
        env_file (str, optional): Path to the dotenv file. Defaults to None.
    """
    load_dotenv(env_file, override=True)

    # environment variables to connect to mongodb
    tweepipe.settings.MONGO_DB = os.getenv("MONGO_DB")
    tweepipe.settings.MONGO_USERNAME = os.getenv("MONGO_USERNAME")
    tweepipe.settings.MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
    tweepipe.settings.MONGO_HOST = os.getenv("MONGO_HOST")
    tweepipe.settings.MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))

    # connect to a slack channel to notify of data collection events
    tweepipe.settings.SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
    tweepipe.settings.DATA_COLLECTION_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")

    # relative to the current home dir
    tweepipe.settings.OUTPUT_DIR = os.getenv("OUTPUT_DIR")
    if tweepipe.settings.OUTPUT_DIR != None:
        tweepipe.settings.OUTPUT_DIR = tweepipe.settings.SNPIPELINE_HOME.joinpath(
            tweepipe.settings.OUTPUT_DIR
        )
        tweepipe.settings.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    else:
        tweepipe.settings.OUTPUT_DIR = tweepipe.settings.SNPIPELINE_HOME

    # environment variable to connect to academic api, use search tweets
    tweepipe.settings.ACADEMIC_API_BEARER_TOKEN = os.getenv("ACADEMIC_API_BEARER_TOKEN")
    tweepipe.settings.ACADEMIC_API_CONSUMER_KEY = os.getenv("ACADEMIC_API_CONSUMER_KEY")
    tweepipe.settings.ACADEMIC_API_CONSUMER_SECRET = os.getenv(
        "ACADEMIC_API_CONSUMER_SECRET"
    )

    # load standard
    tweepipe.settings.ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
    tweepipe.settings.ACCESS_TOKEN_SECRET = os.getenv("ACCESS_TOKEN_SECRET")
    tweepipe.settings.CONSUMER_KEY = os.getenv("CONSUMER_KEY")
    tweepipe.settings.CONSUMER_SECRET = os.getenv("CONSUMER_SECRET")
    tweepipe.settings.TWITTER_CREDENTIALS_V1 = {
        "access_token": tweepipe.settings.ACCESS_TOKEN,
        "access_token_secret": tweepipe.settings.ACCESS_TOKEN_SECRET,
        "consumer_key": tweepipe.settings.CONSUMER_KEY,
        "consumer_secret": tweepipe.settings.CONSUMER_SECRET,
    }

    # load path to chrome drive for debugging purposes
    tweepipe.settings.CHROME_DRIVER_EXE = os.getenv("CHROME_DRIVER_EXE")


def load_academic_api_credentials(env_file: str = None):
    """
    Directly load credentials to the academic api.

    Args:
        env_file (str, optional): Path to the dotenv file. Defaults to None.
    """
    load_dotenv(env_file, override=True)

    # environment variable to connect to academic api, use search tweets
    tweepipe.settings.ACADEMIC_API_BEARER_TOKEN = os.getenv("ACADEMIC_API_BEARER_TOKEN")
    tweepipe.settings.ACADEMIC_API_CONSUMER_KEY = os.getenv("ACADEMIC_API_CONSUMER_KEY")
    tweepipe.settings.ACADEMIC_API_CONSUMER_SECRET = os.getenv(
        "ACADEMIC_API_CONSUMER_SECRET"
    )
