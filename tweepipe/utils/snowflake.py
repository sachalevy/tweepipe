from datetime import datetime


class SnowFlake:
    """
    Reverse engineering of the tweet id generation mechanism, based
    on current timestamp and integer offset.

    :return: defaults to None.
    :rtype: None.
    """

    @classmethod
    def get_timestamp_from_tweet_id(cls, tweet_id: int):
        """Get tweet id timestamp.

        :param tweet_id: Tweet id to reverse engineer.
        :type tweet_id: int
        :return: Integer timestamp corresponding to tweet id.
        :rtype: int
        """

        offset = 1288834974657
        tstamp = (tweet_id >> 22) + offset

        return tstamp

    @classmethod
    def get_tweet_id_from_time(cls, time_obj: datetime):
        """Convert time specs into a tweet id.

        :param time_obj: Datetime object to convert to tweet id.
        :type time_obj: datetime
        :return: Inferred tweet id.
        :rtype: int
        """

        offset = 1288834974657
        ts = int(datetime.timestamp(time_obj) * 1000)
        ts -= offset
        tweet_id = ts << 22

        return tweet_id

    @classmethod
    def get_datetime_from_tweet_id(cls, tweet_id: int):
        """Retrieve datetime object from tweet id.

        :param time_obj: Tweet id to reverse engineer.
        :type time_obj: int
        :return: Inferred datetime from tweet id.
        :rtype: datetime
        """

        ts = cls.get_timestamp_from_tweet_id(tweet_id)
        utcdttime = datetime.utcfromtimestamp(ts / 1000)

        return utcdttime
