# Change Log

## [0.3.6]

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

