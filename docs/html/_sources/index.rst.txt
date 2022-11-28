.. Snpipeline documentation master file, created by
   sphinx-quickstart on Sun Jan  9 19:15:36 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Snpipeline's documentation!
======================================

Snpipeline is a Twitter data collection toolkit. It contains scripts and modules that have been
useful to me (Sacha) over the past two years. Snpipeline **does not** implement a new way of
retrieving data from Twitter (unlike Twint). It is largely based on the Twitter API. However,
Snpipeline **does** strive to make collecting Twitter data *faster* and *easier*. In particular, it
enables parallel data collection using multiple Twitter API credentials, and data storage with MongoDB.
 
Snpipeline notably extends the following Tweepy features: 

- Tweet stream (v1.1)
- Tweet search (v1.1, v2)
- User profile lookup (v1.1, v2)
- User follower lookup (v1.1)
- User likes lookup (v1.1, v2)
- Tweet likes lookup (v1.1, v2)
- Tweet lookup (v1.1, v2)

Snpipeline also enables more complex use cases:

- User activity checker (are the users in my dataset still on Twitter?)
- Bulk tweet hydration (hydrate a large dataset of tweet ids)
- Time-sampled full archive search (cover your entire study's time range without exceeding quotas)
- Bot likelihood estimation (a quick and approximative take bot detection)


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   snpipeline
   scripts

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
