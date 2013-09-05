import sbt._
import Keys._

object SodaFountainWar {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++
    com.earldouglas.xsbtwebplugin.WebPlugin.webSettings ++
    Seq(
      libraryDependencies ++= Seq(
        "javax.servlet" % "servlet-api" % "2.5" % "provided",
        "org.mortbay.jetty" % "jetty" % "6.1.22" % "container"
      )
    )
}
