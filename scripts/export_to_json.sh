HOST="localhost"
PORT=27017
DATABASE="CONGRESSV2"


mongoexport --host=$HOST --port=$PORT --db=$DATABASE --collection="hashtag_relations" --out="data/congress_hashtag_relations.json"
mongoexport --host=$HOST --port=$PORT --db=$DATABASE --collection="retweet_relations" --out="data/congress_retweet_relations.json"
mongoexport --host=$HOST --port=$PORT --db=$DATABASE --collection="mention_relations" --out="data/congress_mention_relations.json"
mongoexport --host=$HOST --port=$PORT --db=$DATABASE --collection="quote_relations" --out="data/congress_quote_relations.json"
mongoexport --host=$HOST --port=$PORT --db=$DATABASE --collection="tweets" --out="data/congress_tweets.json"
mongoexport --host=$HOST --port=$PORT --db=$DATABASE --collection="users" --out="data/congress_users.json"