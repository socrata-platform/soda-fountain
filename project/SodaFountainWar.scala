import sbt._
import Keys._
import Dependencies._

object SodaFountainWar {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++
    com.earldouglas.xsbtwebplugin.WebPlugin.webSettings ++
    Seq(
      libraryDependencies ++= Seq(javaxServletApi, mortbayJetty)
    )
}
