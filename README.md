**soda-fountain** is Socrata's HTTP REST server for serving Socrata Open Data API (SODA) version 2 API requests.  It talks to two other REST microservices, the data-coordinator, and the query-coordinator, to manage both CRUD operations on datasets as well as queries against datasets.

## Projects

* soda-fountain-lib -- most of the code lives here, including routes, request handling, etc.
* soda-fountain-jetty - a Main for running within embedded Jetty
* soda-fountain-war - stuff for running in an application container

## Running

For fast dev cycles:

1. Add `-Dconfig.file=/etc/soda2.conf` to `SBT_OPTS` and populate it
2. `sbt "soda-fountain-jetty/run"`

To build an assembly and run as a separate process:

    bin/start_soda_fountain.sh

## Tests

`sbt "soda-fountain/test"`

For soda-fountain-lib, log output goes to `sbt-test.log`.  Logging is controlled via `soda-fountain-lib/src/test/resources/log4j.properties`.

## Migrations

Using sbt:
`sbt -Dconfig.file=/etc/soda2.conf "soda-fountain-jetty/run-main com.socrata.soda.server.MigrateSchema [command] [numberOfChanges]"`

To build and run migrations from command line:
`bin/run_migrations.sh`

##### Commands: 
* migrate - apply all migrations to the database
* undo - rollback the latest change, or [numberOfChanges] if specified
* redo - runs undo then migrate in one command
