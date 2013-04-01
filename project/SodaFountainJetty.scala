import sbt._
import Keys._

object SodaFountainJetty{
  lazy val settings: Seq[Setting[_]] = BuildSettings.buildSettings ++ Seq(
    libraryDependencies += "com.socrata" %% "socrata-http-jetty" % "1.2.0"
  )
}