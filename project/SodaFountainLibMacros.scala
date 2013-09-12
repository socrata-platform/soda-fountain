import sbt._
import Keys._

object SodaFountainLibMacros {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies ++= Seq(
      "com.rojoma" %% "rojoma-json" % "[2.4.0,3.0.0)"
    ),
    resolvers += Resolver.sonatypeRepo("snapshots"),
    addCompilerPlugin("org.scala-lang.plugins" % "macro-paradise_2.10.2" % "2.0.0-SNAPSHOT")
  )
}
