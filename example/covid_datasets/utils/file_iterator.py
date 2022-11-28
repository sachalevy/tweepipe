import json
import queue
from pathlib import Path

from loguru import logger
from tweepipe.utils import snowflake


class TweetFileIterator:
    def __init__(self, filepath: Path, batch_size: int = 2048):
        self.filepath = filepath

        self.batch_queue = queue.Queue()
        self.batch_size = batch_size

        self.last_day = 30
        self.fileformat = None
        self.line_parser = None

        self.progress_file = self.filepath.joinpath(".download_progress.json")
        try:
            self.load_progress()
        except Exception:
            # month starts with day 1
            self.day = 1
            self.hour = 0

    def __iter__(self):
        return self

    def update_progress(self):
        """Update progress file with latest day stored. Note as cannot guarantee all file has been
        uploaded to db, if process file, will try reloading the last loaded day.
        """

        with open(str(self.progress_file), "w") as f:
            json.dump({"day": (self.day - 1)}, f)

    def load_progress(self):
        with open(str(self.progress_file), "r") as f:
            self.day = json.load(f).get("day")

    def __next__(self):
        """Iterate over all segments of tids available in Geocov files."""
        if self.batch_queue.empty() and self.day > 30:
            return None

        # reload queue with next day if needed
        if self.batch_queue.empty():
            self.load_next_batch()

        return self.batch_queue.get()

    def load_next_batch(self):
        """Load next day of data from file."""

        tmp_filename = self.filepath.joinpath(self.fileformat.format(day=self.day))
        logger.info(f"Reading new data for {self.day:02}: {str(tmp_filename)}.")
        tmp_tweet_batch = []
        with open(tmp_filename, "r") as f:
            for line in f:
                try:
                    tmp_tweet_batch.append(self.get_tweet_doc(line))
                except ValueError as e:
                    continue
                if len(tmp_tweet_batch) % self.batch_size == 0:
                    self.batch_queue.put(tmp_tweet_batch)
                    tmp_tweet_batch = []

        if len(tmp_tweet_batch) > 0:
            self.batch_queue.put(tmp_tweet_batch)

        # increment day count for iteration
        self.day += 1
        self.update_progress()

    def get_tweet_doc(self, line: str):
        """Parse retrieved tweet id line to string, and deduce post date."""

        tid = self.line_parser(line)

        return {
            "tid": tid,
            "status": 0,
            "created_at": snowflake.get_datetime_from_tweet_id(int(tid)),
        }


class EchenData(TweetFileIterator):
    def __init__(self, filepath: Path, batch_size: int = 2048):
        TweetFileIterator.__init__(self, filepath, batch_size)

        # iterate all the files by hour
        self.line_parser = lambda x: x.strip()
        self.fileformat = "coronavirus-tweet-id-2020-04-{day:02}-{hour:02}.txt"

    def update_progress(self):
        with open(str(self.progress_file), "w") as f:
            json.dump({"day": self.day, "hour": (self.hour - 1) % 24}, f)

    def load_progress(self):
        with open(str(self.progress_file), "r") as f:
            progress = json.load(f)
            self.day = progress.get("day")
            self.hour = progress.get("hour")

    def load_next_batch(self):
        """Load next batch of Echen file tweet ids."""

        tmp_filename = self.filepath.joinpath(
            self.fileformat.format(day=self.day, hour=self.hour)
        )
        logger.info(
            f"Reading new data for {self.day:02}:{self.hour:02} : {str(tmp_filename)}."
        )
        tmp_tweet_batch = []
        with open(tmp_filename, "r") as f:
            for line in f:
                try:
                    tmp_tweet_batch.append(self.get_tweet_doc(line))
                except ValueError as e:
                    continue
                if len(tmp_tweet_batch) % self.batch_size == 0:
                    self.batch_queue.put(tmp_tweet_batch)
                    tmp_tweet_batch = []

        if len(tmp_tweet_batch) > 0:
            self.batch_queue.put(tmp_tweet_batch)

        # increment day count for iteration
        self.hour = (self.hour + 1) % 24
        if self.hour == 0:
            self.day += 1

        self.update_progress()


class PanaceaData(TweetFileIterator):
    def __init__(self, filepath: Path, batch_size: int = 2048):
        TweetFileIterator.__init__(self, filepath, batch_size)

        self.line_parser = lambda x: x.split()[0]
        self.fileformat = "2020-04-{day:02}/2020-04-{day:02}-dataset.tsv"


class GeocovData(TweetFileIterator):
    def __init__(self, filepath: Path, batch_size: int = 2048):
        TweetFileIterator.__init__(self, filepath, batch_size)

        self.line_parser = lambda x: x.split()[0]
        self.fileformat = "en_ids_2020-04-{day:02}.tsv"
