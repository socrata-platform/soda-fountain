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

    sbt assembly
    java -Dconfig.file=/etc/soda2.conf -jar soda-fountain-jetty/target/scala-2.10/soda-fountain-jetty-assembly-0.0.16-SNAPSHOT.jar &

## Tests

`sbt "soda-fountain/test"`

## Migrations

Using sbt:
sbt -Dconfig.file=/etc/soda2.conf "soda-fountain-jetty/run-main com.socrata.soda.server.MigrateSchema [command] [numberOfChanges]"

Using an assembly jar:
java -Dconfig.file=/etc/soda2.conf -cp soda-fountain-jetty/target/scala-2.10/soda-fountain-jetty-assembly-0.0.16-SNAPSHOT.jar com.socrata.soda.server.MigrateSchema [command] [numberOfChanges]

##### Commands: 
* Migrate - apply all migrations to the database
* Undo - rollback the latest change, or [numberOfChanges] if specified
* Redo - runs undo then migrate in one command
