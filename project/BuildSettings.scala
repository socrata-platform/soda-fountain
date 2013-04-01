import sbt._
import Keys._

object BuildSettings {
  val buildSettings: Seq[Setting[_]] = Defaults.defaultSettings ++ Seq(
    organization := "com.socrata",
    version := "0.0.15-SNAPSHOT",
    scalaVersion := "2.10.0",
    testOptions in Test ++= Seq(
      Tests.Argument(TestFrameworks.ScalaTest, "-oFD")
    ),
    scalacOptions ++= Seq("-encoding", "UTF-8", "-g:vars", "-deprecation", "-feature", "-language:implicitConversions"),
    javacOptions ++= Seq("-encoding", "UTF-8", "-g", "-Xlint:unchecked", "-Xlint:deprecation", "-Xmaxwarns", "999999"),
    ivyXML := // com.rojoma and com.socrata have binary compat guarantees
      <dependencies>
        <conflict org="com.socrata" manager="latest-compatible"/>
        <conflict org="com.rojoma" manager="latest-compatible"/>
      </dependencies>,
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "jcl-over-slf4j" % slf4jVersion,
      "org.scalatest" %% "scalatest" % "1.9.1" % "test"
    )
  )

  val slf4jVersion = "1.7.5"
}
