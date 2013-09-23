import sbt._
import Keys._

object SodaFountainJetty {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies ++= Seq(
      "com.socrata" %% "socrata-http-jetty" % "2.0.0-SNAPSHOT",
      "com.socrata" %% "socrata-http-curator-broker" % "2.0.0-SNAPSHOT"
    )
  )
}
