[![pandapower](https://www.pandapower.org/images/pp.svg)](https://www.pandapower.org)         [![pandapipes](https://www.pandapipes.org/images/pp.svg)](https://www.pandapipes.org)

[![pandahub](https://badge.fury.io/py/pandahub.svg)](https://pypi.org/project/pandahub/) [![pandahub](https://img.shields.io/pypi/pyversions/pandahub.svg)](https://pypi.org/project/pandahub/) [![pandahub](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://github.com/e2nIEE/pandahub/blob/master/LICENSE)

pandahub is a data hub for pandapower and pandapipes networks based on MongoDB. It allows you to store pandapower and
pandapipes networks as well as timeseries in a MongoDB. pandahub allows you to access the database directly through the PandaHub class,
but also provides a REST-API based on FastAPI. Access through the API is managed with a user management implementation based on FastAPI Users.

## Development
`docker compose up -d` runs a mongodb container alongside a pandahub api instance with live reload available
at http://localhost:8002. To connect to an existing database instead, set `MONGODB_URL` to the connection string through an environment variable / in you `.env` file.

Swagger UI is available at http://localhost:8002/docs.

If you develop on the library and do not need the fastapi app, `docker compose up db -d` starts only the mongodb
container.
