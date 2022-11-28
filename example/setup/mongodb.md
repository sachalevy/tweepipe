# MongoDB

How to setup and connect to your MongoDB instance.

## Installation
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


## Connecting to the MongoDB instance:

```
mongo \
	-u username \
	-p password \
	--host host
```