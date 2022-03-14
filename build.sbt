ThisBuild / organization := "com.socrata"

ThisBuild / scalaVersion := "2.12.8"

ThisBuild / resolvers ++= Seq(
  "socrata" at "https://repo.socrata.com/artifactory/libs-release"
)

ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature")
ThisBuild / evictionErrorLevel := Level.Warn

val sodaFountainLibMacros = project in file("soda-fountain-lib-macros")

val sodaFountainExternal = project in file("soda-fountain-external")

val sodaFountainLib = (project in file("soda-fountain-lib")).
  dependsOn(sodaFountainLibMacros, sodaFountainExternal)

val sodaFountainJetty = (project in file("soda-fountain-jetty")).
  dependsOn(sodaFountainLib)

releaseProcess -= ReleaseTransformations.publishArtifacts

disablePlugins(AssemblyPlugin)
