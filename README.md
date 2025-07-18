[![pandapower](https://www.pandapower.org/images/pp.svg)](https://www.pandapower.org)         [![pandapipes](https://www.pandapipes.org/images/pp.svg)](https://www.pandapipes.org)

[![pandahub](https://img.shields.io/pypi/v/pandahub?label=pypi%20package&color=green)](https://pypi.org/project/pandahub/) [![pandahub](https://img.shields.io/pypi/pyversions/pandahub.svg)](https://pypi.org/project/pandahub/) [![pandahub](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://github.com/e2nIEE/pandahub/blob/master/LICENSE)

pandahub is a data hub for pandapower and pandapipes networks based on MongoDB. It allows you to store pandapower and
pandapipes networks as well as timeseries in a MongoDB. pandahub allows you to access the database directly through the
PandaHub class, but also provides a REST-API based on FastAPI including authentication (implemented with FastAPI Users).

## PandaHub example
Run `uv sync` and bring up a local mongodb instance with `docker compose up db -d`.

Save pandapower's `mv_oberrhein` network to the database:
```pycon
>>> net = pp.networks.mv_oberrhein()
>>> ph = PandaHub()
>>> project_data = ph.create_project("MV Oberrhein Example")
>>> net_data = ph.write_network_to_db(net, name="mv_oberrhein", net_id=0)
```
Close a switch in the database:
```pycon
>>> open_switch_idx = net.switch.query("closed==False").index.to_list()[0]
>>> ph.set_net_value_in_db(net_id=0, element_type="switch", element_index=open_switch_idx,
                           parameter="closed", value=True)

```
Retrieve the network from the database and confirm the switch is now closed:
```pycon
>>> net_from_db = ph.get_network(net_id=0)
>>> net.switch.loc[open_switch_idx, "closed"]
np.False_
>>> net_from_db.switch.loc[open_switch_idx, "closed"]
np.True_
```

## REST-api
Run `uv sync --extra rest-api` to set up your venv with the required dependencies, then build the docker image and bring up a
mongodb database + the rest-api:

```console
docker compose build
docker compose up -d
```
This will run the example app in `pandahub/api/main.py` with live reload at http://localhost:8002.
To connect to an existing database instead, set `MONGODB_URL` to the connection string through an environment variable or in you `.env` file.

Swagger UI is available at http://localhost:8002/docs.

## MongoClient handling

You can supply a mongodb connection string (or the host as connection string and user/password separately) either as arguments to PandaHub
or by setting the following environment variables:

* MONGODB_URL
* MONGODB_USER (optional)
* MONGODB_PASSWORD (optional)

These values are then used as default arguments when creating a PandaHub object.

By default, each instantiation of a PandaHub instance will create a new MongoClient instance. This is fine for interactive use or small scripts. To clean up the mongo client, call `ph.close()` on the instance once you are done with it.

When running the pandahub API as a web application, provide database credentials through the environment variables and additionally set `PANDAHUB_GLOBAL_DB_CLIENT` to `true`.
A single, global mongodb client instance per process will then be used for all PandaHub instantiations, as recommended by [pymongo](https://pymongo.readthedocs.io/en/stable/faq.html#how-does-connection-pooling-work-in-pymongo).

You can use the following properties to access database resources from a PandaHub object:

* *ph.project_db*: database of the activated project (e.g. `ph.project_db.net_bus.find().to_list()`)
* *ph.mgmt_db*: the user_management database
* *ph.users_collection*: the users collection from the management database
* *ph.projects_collection*: the projects collection from the management database


In case you want to access the database but do not need a PandaHub object, there are two convenience functions available:

```python
from pandahub.lib import get_mongo_client

my_collection = get_mongo_client("db", "collection")
docs = collection.find().to_list()
```
`PANDAHUB_GLOBAL_DB_CLIENT` is respected, you either get the global client or a fresh instance, in which case you are responsible for cleaning up the connection.

```python
from pandahub.lib import mongo_client

with mongo_client("db", "my_collection") as collection:
    docs = collection.find().to_list()
```
With the context manager, a new MongoClient is created (even if `PANDAHUB_GLOBAL_DB_CLIENT` is `true`) and closed on completion of the with block.
