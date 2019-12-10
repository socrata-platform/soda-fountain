import Dependencies._

name := "soda-fountain-lib-macros"

libraryDependencies ++= Seq(rojomaJson)

addCompilerPlugin("org.scalamacros" %% "paradise" % "2.1.0" cross CrossVersion.full)
