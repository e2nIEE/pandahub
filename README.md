[![pandapower](https://www.pandapower.org/images/pp.svg)](https://www.pandapower.org)         [![pandapipes](https://www.pandapipes.org/images/pp.svg)](https://www.pandapipes.org)

[![pandahub](https://badge.fury.io/py/pandahub.svg)](https://pypi.org/project/pandahub/) [![pandahub](https://img.shields.io/pypi/pyversions/pandahub.svg)](https://pypi.org/project/pandahub/) [![pandahub](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://github.com/e2nIEE/pandahub/blob/master/LICENSE)


pandahub is a data hub for pandapower and pandapipes networks based on MongoDB. It allows you to store pandapower and pandapipes networks as well as timeseries in a MongoDB. pandahub allows you to access the database directly for local development, but also includes a client/server architecture that exposes all relevant methods as an API based on FastAPI. Access through the API is managed with a user management implementation based on FastAPI Users.

## Development
`docker compose up -d` runs a mongodb container alongside a pandahub api instance with live reload available
at http://localhost:8002. To connect to an existing database instead, set `MONGODB_URL` to the connection string through an environment variable / in you `.env` file.

Swagger UI is available at http://localhost:8002/docs.

If you develop on the library and do not need the fastapi app, `docker compose up db -d` starts only the mongodb
container.

## Use pandahub api with pandahub client

1. Login with the pandahub client

   There are two ways to login with pandahub client

   - If you installed pandahub by pip or with setup.py just run `pandahub-login` in your shell.

   OR

   - Run the following in your python or IPython shell:

   ```
   from pandahub.client.user_management import login
   login()
   ```

   This will guide you through the login process.

2. You only need to login once. After you logged in successfully a configuration file (pandahub.config) containing an authentication token is created in your home directory and will be used every time you use the pandahub client. You only need to login again if you want to use a different instance of the pandahub api.
