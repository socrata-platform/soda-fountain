import sbt._
import Keys._

object SodaFountainLib{
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies ++= Seq(
      "com.socrata" %% "coordinator" % "0.0.1-SNAPSHOT",
      "com.socrata" %% "socrata-http-utils" % "1.2.0",
      "com.rojoma" %% "rojoma-json" % "[2.3.0,3.0.0)",
      "net.databinder.dispatch" %% "dispatch-core" % "0.10.0",
      "javax.servlet" % "servlet-api" % "2.5" % "provided",
      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test,it",
      "org.scalatest" %% "scalatest" % "1.9.1" % "test,it"
    )
  )

  lazy val configurations: Seq[Configuration] = BuildSettings.projectConfigurations
}