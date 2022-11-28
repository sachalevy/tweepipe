# BotSpot

**This model is now archived**. A reproduction of the Botometer Lite model.

It attributes a normalized score to a user given their Twitter profile (as per the information returned by the Standard API v1.1).

## Legacy

This model is now archived, and presented for informative purposes. As presented in the Botometer paper, this model originally employed several manually labelled datasets to derive labels for Twitter users engaging in bot-like activity. However, it appears these datasets are not representative anymore of the activity patterns on Twitter.

## Getting Started

We are going to walk through the steps required to score a set of Twitter users. 

### Prerequisites

This example will assume that you have a python environment setup, with the accurate dependencies and the tweepipe module installed in your environment. If this is not the case, please refer to the README from the tweepipe top level directory for instructions to setup the `tweepipe` python environment.

Make sure that your database credentials are available in a `.env` file in your working directory before proceeding. My `.env` file looks like this:

```bash
MONGO_HOST="localhost"
MONGO_PORT=27017
MONGO_USERNAME="username"
MONGO_PASSWORD="password"
```

### Which users?

The first step is to retrieve our user data for the botspot analysis. Botspot currently does not provide a feature to directly analyse users from our databases. Let's say that we have a list of user ids we wish to analyse. We'll see how to use the tweepipe API to retrieve their profile either from one of our databases or from Twitter (in case we don't have them stored in our database).

First, define a list of users, and load the environment for the tweepipe client with a legacy v1.1 API (tweepy < 4.0.0):

```python
from tweepipe import settings
from tweepipe.legacy import client
settings.load_config(env_file=".env")

# define our list of uids
uids = ["800111181058838528", "1284651818782535680", "859219800"]
my_client = client.LegacyClient()
```
> These 3 user ids were taken from tweets in the `example_issue` database.

Let's retrieve their profiles from the `users` collection in the `example_issue` database. We'll use the tweepipe API to do so.

```python
# ... query database for user profiles
source_issue = "example_issue"
profiles = my_client.get_user_profiles(uids, db=source_issue)
```

Let's imagine that the user profiles were not available in the `users` collection, and that we need to retrieve their data from Twitter.

```python
# ... query Twitter for user profiles
botspot_issue = "newusers_botspot"
my_client.lookup_users(uids=uids, issue=botspot_issue, api_count=1)
profiles = my_client.get_user_profiles(uids, db=botspot_issue)
```
> The api first queries Twitter to retrieve user profile data. It then queries the newly created database for the user profiles.

### Use botspot

Now that we have the profiles for our users, we can move to getting their bot-likelihood score using botspot.

```python
# ...
from tweepipe.legacy.botspot import botspot

my_botspot = bostpot.BotSpot()
scores = my_botspot.get_users_scores(profiles)
```
> This method returns a dictionary mapping the user ids to their bot-likelihood.

You may find these couple lines of code in the `example.py` file in the botspot directory. If you want to try it out, feel free to import your own user ids, specify your database of interest and run your queries.

## Maintainers
- Sacha Lévy (sacha.levy@mail.mcgill.ca)