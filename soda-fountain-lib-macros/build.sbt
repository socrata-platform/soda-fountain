import Dependencies._

name := "soda-fountain-lib-macros"

libraryDependencies ++= Seq(rojomaJsonGrisu)

addCompilerPlugin("org.scalamacros" %% "paradise" % "2.1.0" cross CrossVersion.full)

disablePlugins(AssemblyPlugin)
