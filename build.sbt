ThisBuild / organization := "com.socrata"

ThisBuild / scalaVersion := "2.12.8"

ThisBuild / resolvers ++= Seq(
  "socrata" at "https://repo.socrata.com/artifactory/libs-release"
)

ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature")

val sodaFountainMessageLib = project in file("soda-fountain-message-lib")

val sodaFountainLibMacros = project in file("soda-fountain-lib-macros")

val sodaFountainLib = (project in file("soda-fountain-lib")).
  dependsOn(sodaFountainMessageLib, sodaFountainLibMacros)

val sodaFountainJetty = (project in file("soda-fountain-jetty")).
  dependsOn(sodaFountainLib)

val sodaFountainExternal = project in file("soda-fountain-external")

