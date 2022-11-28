from datetime import datetime

import re
import joblib
import numpy as np
from typing import Union

from tweepipe import settings
from tweepipe.db import db_client, db_schema
from tweepipe.legacy.botspot.bfreq import _get_bfreq


class BotSpot:
    """Bot Detection Model."""

    def __init__(self, issue="botspot", model_file_path=None):
        """
        Reproduction of the Botometer Lite model.

        Args:
            issue (str, optional): Name of the issue. Defaults to "botspot".
            model_file_path (_type_, optional): Path to mdel. Defaults to None.
        """
        self.issue = issue
        self.db_conn = db_client.DBClient(
            issue=issue,
            schema=db_schema.BOTSPOT_V1,
        )
        self.botspot = joblib.load(model_file_path or settings.BOTSPOT_MODEL)

    def score_user_batch(self, users: Union[list, set]):
        scores = []
        for user in users:
            scores.append(
                {
                    "uid": user.get("id_str"),
                    "username": user.get("screen_name"),
                    "score": float(self.get_user_score(user)),
                }
            )

        self.db_conn.add_scores(scores)

    def get_users_scores(self, users):
        """Users should be represented by full user profiles."""
        return {
            user["id_str"]: float(self.get_user_score(user)) for user in users if user
        }

    def get_user_score(self, user):
        feature_vec = self._get_user_features(user)
        return self.botspot.predict_proba(feature_vec.reshape(-1, len(feature_vec)))[
            0, 1
        ]

    def _get_screen_name_likelihood(self, name):
        bigram_frequencies = [_get_bfreq(name[i : i + 2]) for i in range(len(name) - 1)]
        if len(bigram_frequencies) > 0:
            return np.mean(np.array(bigram_frequencies))
        else:
            return 0.0

    def _get_user_features(self, user, cap_value=1000000):
        feature_vec = np.zeros(20)

        created_at = user["created_at"]
        creation_time = datetime.strptime(created_at, "%a %b %d %H:%M:%S +0000 %Y")
        probe_time = datetime.now()
        age = probe_time - creation_time
        user_age = age.days * 24 + 1

        feature_vec[0] = user["statuses_count"]
        feature_vec[1] = user["followers_count"]
        feature_vec[2] = user["friends_count"]
        feature_vec[3] = user["favourites_count"]
        feature_vec[4] = user["listed_count"]
        feature_vec[5] = 1 if user["default_profile"] else 0
        feature_vec[6] = 1 if user["profile_use_background_image"] else 0
        feature_vec[7] = 1 if user["verified"] else 0
        feature_vec[8] = max(user["statuses_count"] / user_age, cap_value)
        feature_vec[9] = max(user["followers_count"] / user_age, cap_value)
        feature_vec[10] = max(user["friends_count"] / user_age, cap_value)
        feature_vec[11] = max(user["favourites_count"] / user_age, cap_value)
        feature_vec[12] = max(user["listed_count"] / user_age, cap_value)
        feature_vec[13] = user["followers_count"] / max(user["friends_count"], 1)
        feature_vec[14] = len(user["screen_name"])
        feature_vec[15] = len(re.sub("[^0-9]", "", user["screen_name"]))
        feature_vec[16] = len(user["name"])
        feature_vec[17] = len(re.sub("[^0-9]", "", user["name"]))
        feature_vec[18] = len(user["description"])
        feature_vec[19] = self._get_screen_name_likelihood(user["screen_name"])

        return feature_vec
