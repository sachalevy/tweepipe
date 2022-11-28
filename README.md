# Tweepipe

Easy data collection from Twitter and storage to MongoDB.


## Python Env
You need to have python downloaded, and optionally conda. I highly recommend having conda installed (it makes everything much simpler to keep track of). You can check out how to install miniconda [here](https://docs.conda.io/en/latest/miniconda.html).

Once you finish the installation of miniconda, open a new shell. You should see that your current python environment is now `base` (displayed next to your username in your terminal).

We'll now create a custom python environment for the `tweepipe` module:

```bash
conda env create -f environment.yml
```
> Make sure you are at the root of the tweepipe working directory.

Once the environment is created, add `tweepipe` to your modules as follows:

```bash
conda develop .
```
> Again, make sure you are at the root of the tweepipe working directory.

If you successfully completed these steps, you can skip the `Installation` section below.
If you open a python shell and run the following command, they should execute without any error:

```python
import tweepipe
```

## Using this library

### Installation

Authenticate to Anaconda Cloud and install the package in your environment.

```bash
anaconda login
conda install -c complexdatalab tweepipe
```

### Using MongoDB

This project uses mongodb to store retrieved information. Using the `mongowrapper` package, it simply creates (or exploits existing) required db and collections. Please follow instructions provided on the `mongowrapper` project page for more information. 

```bash
MONGO_DB = <database name>
MONGO_HOST = <database host>
MONGO_PORT = <database port>
# optional if not working locally...
MONGO_USERNAME = <username>
MONGO_PASSWORD = <password>
```

Note that you will need environment variables in your python env to connect to the db (which can be specified in a .env file located at the root of your project). In case you want to store the collected data in a local mongodb, you will also need a running mongodb server.  

## Contributing to this library

### Prerequisites

- python 3.8
- conda
- git

### Initial Setup

Clone the repository and create a new conda environment containing all dependencies:
*yet to be implemented*

```bash
git clone path_to_git_repository
cd tweepipe/

# Create the Conda environment called 'htpipeline'
conda env create -n tweepipe -f environment.yml

# Activate the environment
conda activate tweepipe

# Set the project root to python path (development mode)
conda develop -n tweepipe .
```

You can also update the environment dependencies by runing `conda env update -n tweepipe -f environment.yml`.

You will need a `.env` file at the root of this repository containing all necessary environment variables. Ask a maintainer of this project for this file.

### General contribution guidelines

- Start a new feature branch from the master git branch. Give it a meaningful name.
- Write your code.
- Add unit tests.

### Connecting to the MongoDB instance:

```
mongo \
	-u username \
	-p password \
	--host host
```

## Maintainers

- [Sacha Lévy](https://twitter.com/sachalevy3) (sacha.levy@mail.mcgill.ca)
