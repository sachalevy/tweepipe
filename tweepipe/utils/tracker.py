from loguru import logger
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from tweepipe import settings


class SlackLogger:
    """Log data collection events to slack."""

    @classmethod
    def log_start_task(cls, task: str, msg: str = None):
        """Log the begining of a new task to slack.

        :param task: Name of the started task.
        :type task: str
        :param msg: Message attached to starting the task, defaults to None.
        :type msg: str, optional
        """

        start_msg = f"Task: {task} - Message: {msg}"
        cls.post_message(start_msg)

    @classmethod
    def log_error(cls, error: str, msg: str = None):
        """Log error on ongoing data collection task.

        :param error: Error
        :type error: str
        :param msg: Error message, defaults to None.
        :type msg: str, optional
        """

        # build error msg depending on ongoing data collection
        error_msg = f"Task: {settings.CURRENT_TASK} - " if settings.CURRENT_TASK else ""
        error_msg += f"Error: {error} - Message: {msg}"
        cls.post_message(error_msg)

    @classmethod
    def post_message(cls, msg: str = None):
        # init client on each error (should be small)
        slack_client = WebClient(token=settings.SLACK_BOT_TOKEN)

        try:
            _ = slack_client.chat_postMessage(
                channel=settings.DATA_COLLECTION_CHANNEL_ID, text=msg
            )
        except SlackApiError as e:
            logger.error(f"Error occured while posting message: {e}.")
