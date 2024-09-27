# Soda Fountain

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

`bin/start_soda_fountain.sh`

## Tests

`sbt "soda-fountain/test"`

For soda-fountain-lib, log output goes to `sbt-test.log`.  Logging is controlled via `soda-fountain-lib/src/test/resources/log4j.properties`.

For test coverage reports, do `sbt coverage test`.  XML and HTML coverage reports are generated.  Unfortunately due to a bug in scalac, HTML line by line highlighting is broken.

## Migrations

Using sbt:
`sbt -Dconfig.file=/etc/soda2.conf "soda-fountain-jetty/run-main com.socrata.soda.server.MigrateSchema [command] [numberOfChanges]"`

For example:
`sbt -Dconfig.file=/etc/soda2.conf "soda-fountain-jetty/run-main com.socrata.soda.server.MigrateSchema migrate"`

To build and run migrations from command line:
`bin/run_migrations.sh`

### Commands

* migrate - apply all migrations to the database
* undo - rollback the latest change, or [numberOfChanges] if specified
* redo - runs undo then migrate in one command

## Publishing the Library

To update the library:

1. Make a PR, get it approved and merge your commits into main.
1. From main, make a branch and run `sbt release`. This will create two commits to bump the version and create a git tag for the release version.
1. Create a PR on the branch, get it approved and merged to main.
1. Following the merge of the release commits to main, the Jenkins job for the main branch will build the stages "Check for Version Change" and "Publish Library" to publish the library.
