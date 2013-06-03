import sbt._
import Keys._

object SodaFountainPostgres{
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ Seq(
    libraryDependencies +=  "postgresql" % "postgresql" % "9.1-901-1.jdbc4"
  )

  lazy val configurations: Seq[Configuration] = BuildSettings.projectConfigurations
}