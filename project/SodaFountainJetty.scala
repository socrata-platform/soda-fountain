import sbt._
import Keys._

object SodaFountainJetty{
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ Seq(
    libraryDependencies += "com.socrata" %% "socrata-http-jetty" % "[1.2.0,2.0.0)"
  )

  lazy val configurations: Seq[Configuration] = BuildSettings.projectConfigurations
}