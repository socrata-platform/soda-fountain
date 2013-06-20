import sbt._
import Keys._

object SodaFountainLib{
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies ++= Seq(
      "com.socrata" %% "soql-types" % "0.0.16-SNAPSHOT",
      "com.socrata" %% "socrata-http-utils" % "[1.2.0,2.0.0)",
      "com.socrata" %% "socrata-thirdparty-utils" % "[2.0.0,3.0.0)",
      "com.rojoma" %% "rojoma-json" % "[2.3.0,3.0.0)",
      "net.databinder.dispatch" %% "dispatch-core" % "0.10.0",
      "com.typesafe" % "config" % "1.0.0",
      "com.h2database" % "h2" % "1.3.166" % "test,it",
      "com.socrata" %% "socrata-http-curator-broker" % "[1.3.0,2.0.0)",
      "javax.servlet" % "servlet-api" % "2.5" % "provided",
      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test,it",
      "org.scalatest" %% "scalatest" % "1.9.1" % "test,it"
    )
  )

  lazy val configurations: Seq[Configuration] = BuildSettings.projectConfigurations
}