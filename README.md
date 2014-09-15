**soda-fountain** is Socrata's HTTP REST server for serving Socrata Open Data API (SODA) version 2 API requests.  It talks to two other REST microservices, the data-coordinator, and the query-coordinator, to manage both CRUD operations on datasets as well as queries against datasets.

## Projects

* soda-fountain-lib -- most of the code lives here, including routes, request handling, etc.
* soda-fountain-jetty - a Main for running within embedded Jetty
* soda-fountain-war - stuff for running in an application container

## Dependencies
* Zookeeper
* PostgreSQL (stores metadata about datasets)
* Query Coordinator (registered through Zookeeper)
* Data Coordinator (registered through Zookeeper)

## Running

For fast dev cycles:

1. Start SBT shell
2. `reStart --- -Dconfig.file=/etc/soda2.conf`

This forks a separate JVM, killing any previously forked JVM.  For really fast compile and auto run, do `~reStart`.  Much faster than building an assembly!

To build an assembly and run as a separate process:

    bin/start_soda_fountain.sh

## Tests

`sbt "soda-fountain/test"`

For soda-fountain-lib, log output goes to `sbt-test.log`.  Logging is controlled via `soda-fountain-lib/src/test/resources/log4j.properties`.

For test coverage reports, do `sbt soda-fountain-lib/scoverage:test`.  XML and HTML coverage reports are generated.  Unfortunately due to a bug in scalac, HTML line by line highlighting is broken.

## Migrations

Using sbt:
`sbt -Dconfig.file=/etc/soda2.conf "soda-fountain-jetty/run-main com.socrata.soda.server.MigrateSchema [command] [numberOfChanges]"`

To build and run migrations from command line:
`bin/run_migrations.sh`

##### Commands: 
* migrate - apply all migrations to the database
* undo - rollback the latest change, or [numberOfChanges] if specified
* redo - runs undo then migrate in one command
