# Change Log

## 0.5.10
🐛 Bug Fixes
* use `timestamp_range` in function `get_timeseries_from_db` by @dreissland in https://github.com/e2nIEE/pandahub/pull/130
🛠 Improvements
* delete project if last user is removed by @jthurner in https://github.com/e2nIEE/pandahub/pull/131

## 0.5.10
🐛 Bug Fixes
* fix _collection_name_of_element and _element_name_of_collection for elements without net_ prefix (https://github.com/e2nIEE/pandahub/pull/127)
* add missing mongodb indexes for variant collection (https://github.com/e2nIEE/pandahub/pull/128)

## 0.5.9
🛠 Improvements
* Introduce metadata-collections for timeseries collections to enable fast querying of metadata values (https://github.com/e2nIEE/pandahub/pull/124)

## 0.5.8
🐛 Bug Fixes
* remove unnecessary dependencies from base package (https://github.com/e2nIEE/pandahub/pull/123)

## 0.5.7
🐛 Bug Fixes
* respect timeseries range when querying timeseries collections (https://github.com/e2nIEE/pandahub/pull/122)

## 0.5.6
🛠 Improvements
* allow to define line_filter in get_subnet (https://github.com/e2nIEE/pandahub/pull/119)

## 0.5.5
🐛 Bug Fixes
* default project["permissions"] to None in get_projects instead of omitting it (https://github.com/e2nIEE/pandahub/pull/117)

## 0.5.4

🛠 Improvements
* simplify project locking (https://github.com/e2nIEE/pandahub/pull/114)
* improve ph.get_project (https://github.com/e2nIEE/pandahub/pull/113)
* add ph.get_project_names (https://github.com/e2nIEE/pandahub/pull/115)

## 0.5.3

🛠 Improvements
* add type aliases for pandapower / pandapipes networks (https://github.com/e2nIEE/pandahub/pull/112)

## 0.5.2

🛠 Improvements
* split mongodb indexes by pandapower / pandapipes elements (https://github.com/e2nIEE/pandahub/pull/110)

## 0.5.1

🐛 Bug Fixes
* global db client: do not connect to server in the background by @jthurner in https://github.com/e2nIEE/pandahub/pull/107
* allow activating locked project if not held by other user by @jthurner in https://github.com/e2nIEE/pandahub/pull/108

## 0.5.0

🛠 Improvements
* make api-dependencies optional, refactor settings, switch to uv + hatch (https://github.com/e2nIEE/pandahub/pull/101)

🐛 Bug Fixes
* server_is_available crashing (https://github.com/e2nIEE/pandahub/pull/103)
* add mongodb indexes for missing elements (https://github.com/e2nIEE/pandahub/pull/102)

🏗 Chore
* remove user verification and password reset (https://github.com/e2nIEE/pandahub/pull/99)

## [0.4.0]
    - IMPROVED: pandapower 3.0 support

## [0.3.13]
    - FIXED: fetching integer index on empty collection
    - IMPROVED: context manager for mongo client /database / collection
    - IMPROVED: project locking returns True if user already holds a lock on the project

## [0.3.11]

    -  IMPROVED: add lib.database_toolbox.get_mongo_client function

## [0.3.10]

    -  BREAKING: changed signatures of create_variant function and route

## [0.3.9]

    -  BREAKING: inserting multiple networks with the same name does not represent an error anymore, networks are only unique by their net_id (_id field of the collection)
    -  BREAKING: passing net_id as str will not remap to the project name internally but look up on the _id field
    -  IMPROVED: fix race condition when calling write_network_to_db without net_id

## [0.3.8]

    -  BREAKING: specifying variant as list will raise an error instead of a deprecation warning

## [0.3.7]

    -  FIXED: changes introduced in 0.3.6 removed all dependencies from the project
    -  IMPROVED: allow $or and $and in timeseries metadata query

## [0.3.6]
    ! yanked, see 0.3.7

    -  IMPROVED consider realm when activating project by name
    -  IMPROVED housekeeping: clean up project layout, Docker, settings and remove PandaHubClient
    -  IMPROVED access of correct collection in get_net_ids
    -  IMPROVED cleaner project version upgrades (early exit, log spam)

## [0.3.5]

    - Fixed version and repaired pyproject.toml

## [0.2.4]

    - BREAKING drops index argument from create_variant() function

## [0.2.3]- 2022-08-04

    - ADDED version property in project data
    - ADDED method to migrate projects to latest version
    - ADDED option to disable registration
    - ADDED option to use a separate mongodb instance as global database
    - ADDED geo mode to handle geojson columns
    - ADDED tutorials
    - IMPROVED collections for element tables now start with 'net_'
    - IMPROVED project IDs now can be any name
    - IMPROVED compatibility with python < 3.9
    - IMPROVED project settings API
    - IMPROVED timeseries handling

## [0.2.2]- 2022-04-27

   - IMPROVED object support in network dataframes
   - IMPROVED None and 'empty' handling for project metadata
   - FIXED data types not saved for empty networks
   - FIXED element defaults not added during element creation

## [0.2.1]- 2022-04-22

   - FIXED superuser flag not respected when activating project

## [0.2.0]- 2022-04-17

   - ADDED Python 3.7 support
   - ADDED getting/setting project settings and metadata
   - ADDED full database access for superusers
   - ADDED element handling to API and client

## [0.1.3]- 2022-03-31

   - FIXED wrong API routes

## [0.1.2]- 2022-03-29

   - FIXED PyPi links
   - ADDED shields and description

## [0.1.0]- 2022-03-29

   - Initial release

