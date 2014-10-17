import sbt._
import Keys._

import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin.MergeStrategy
import sbtassembly.AssemblyUtils._
import com.socrata.cloudbeessbt.SocrataCloudbeesSbt.SocrataSbtKeys._

object SodaFountainJetty {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ Seq(
    libraryDependencies ++= Seq(
      "com.socrata" %% "socrata-http-jetty" % "2.3.2",
      "com.socrata" %% "socrata-http-curator-broker" % "2.3.2",
      "io.dropwizard.metrics" % "metrics-jetty9"   % "3.1.0",
      "io.dropwizard.metrics" % "metrics-graphite"   % "3.1.0"
    ),
    dependenciesSnippet :=
      <xml.group>
        <exclude org="commons-logging" module="commons-logging"/>
        <exclude org="commons-logging" module="commons-logging-api"/>
      </xml.group>
    ,
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
