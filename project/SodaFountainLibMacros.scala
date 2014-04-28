import sbt._
import Keys._

object SodaFountainLibMacros {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies ++= Seq(
      "com.rojoma" %% "rojoma-json" % "[2.4.3,3.0.0)",
      "org.scalamacros" %% "quasiquotes" % "2.0.0"
    ),
    addCompilerPlugin("org.scalamacros" %% "paradise" % "2.0.0" cross CrossVersion.full)
  )
}
