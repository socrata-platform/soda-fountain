import sbt._
import Keys._

import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin.MergeStrategy
import sbtassembly.AssemblyUtils._

object SodaFountainJetty {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ Seq(
    libraryDependencies ++= Seq(
      "com.socrata" %% "socrata-http-jetty" % "2.0.0-SNAPSHOT",
      "com.socrata" %% "socrata-http-curator-broker" % "2.0.0-SNAPSHOT"
    ),
    mainClass := Some("com.socrata.soda.server.SodaFountainJetty"),
    mergeStrategy in assembly <<= (mergeStrategy in assembly) { old =>
      {
        case "about.html" => MergeStrategy.rename
        case s if s.startsWith("scala/reflect/api/") =>
          MergeStrategy.first // I hope this works...
        case x => old(x)
      }
    }
  )
}
