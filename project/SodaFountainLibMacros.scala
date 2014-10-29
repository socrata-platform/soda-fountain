import sbt._
import Keys._
import Dependencies._

object SodaFountainLibMacros {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies ++= Seq(quasiQuotes, rojomaJson),
    addCompilerPlugin("org.scalamacros" %% "paradise" % "2.0.0" cross CrossVersion.full)
  )
}
