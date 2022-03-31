[![pandapower](https://www.pandapower.org/images/pp.svg)](https://www.pandapower.org)         [![pandapipes](https://www.pandapipes.org/images/pp.svg)](https://www.pandapipes.org)

[![pandahub](https://badge.fury.io/py/pandahub.svg)](https://pypi.org/project/pandahub/) [![pandahub](https://img.shields.io/pypi/pyversions/pandahub.svg)](https://pypi.org/project/pandahub/) [![pandahub](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://github.com/e2nIEE/pandahub/blob/master/LICENSE)


pandahub is a data hub for pandapower and pandapipes networks based on MongoDB. It allows you to store pandapower and pandapipes networks as well as timeseries in a MongoDB. pandahub allows you to access the database directly for local development, but also includes a client/server architecture that exposes all relevant methods as an API based on FastAPI. Access through the API is managed with a user management implementation based on FastAPI Users.

## Setup a local pandahub api

Steps to test the client/server structure locally:

1. Start a MongoDB on localhost:27017 (or another custom port)

2. Start the uvicorn server that exposes the API by navigating to "pandahub/api" and running:

   ```
   # windows
   set SECRET=secret & python main.py

   # linux
   SECRET=secret python main.py
   ```

   or if you don't run mongodb on the default port (27017)

   ```
   # windows
   set MONGODB_URL=mongodb://localhost:[mongo-port] & set SECRET=secret & python main.py

   # linux
   MONGODB_URL=mongodb://localhost:[mongo-port] SECRET=secret python main.py
   ```

   The API should now run on http://localhost:8002

   >**Note**
   >A full documentation of all api endpoints can be seen at http://localhost:8002/docs

   >**Note 2**
   >You can avoid always setting the environment variables for SECRET and MONGODB_URL by creating an `.env` file in `pandahub/api/` with the following content:
   >```
   >SECRET=secret
   >MONGODB_URL=mongodb://localhost:[mongo-port]
   >```

## Develop with Docker

`docker compose up` starts a mongodb container alongside pandahub with live reload available at http://localhost:8002.

If you want to connect to a running database, set the database url and specify only docker-compose.yml:

    MONGODB_URL=mongodb://localhost:[mongo-port] docker compose -f docker-compose.yml up



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
