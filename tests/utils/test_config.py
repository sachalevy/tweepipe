import datetime
import json

from tweepipe.legacy.utils import extract

test_env_file = "tests/utils/.env.test"
test_db_issue = "test_issue"

test_sample_v2_response_file = "tests/utils/sample_academic_response.json"
with open(test_sample_v2_response_file, "r") as f:
    sample_v2_response = json.load(f)

max_results = 10
max_iter = 1
keywords = ["US", "Biden"]

# two hours on january 6 about the US
start_time_str = "Wed Jan 06 21:17:15 +0000 2021"
start_time_ts = extract._get_creation_time_stamp(start_time_str)
end_time_str = "Wed Jan 06 22:18:15 +0000 2021"
end_time_ts = extract._get_creation_time_stamp(end_time_str)

full_archive_raw_output_filename = (
    "raw_full_archive_search_01M06D2021Y_21h17m_01M06D2021Y_22h18m_1_tweets.jsonl"
)
full_archive_output_filename = (
    "full_archive_search_01M06D2021Y_21h17m_01M06D2021Y_22h18m_1_tweets.jsonl"
)

searchargs = {
    "bearer_token": "hello_secret",
    "endpoint": "https://api.twitter.com/2/tweets/search/all",
    "extra_headers_dict": None,
}
