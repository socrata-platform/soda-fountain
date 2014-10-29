import sbt._
import Keys._
import Dependencies._

import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin.MergeStrategy
import sbtassembly.AssemblyUtils._
import com.socrata.cloudbeessbt.SocrataCloudbeesSbt.SocrataSbtKeys._

object SodaFountainJetty {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ Seq(
    libraryDependencies ++= Seq(
      // See CORE-3635: use lower version of graphite to work around Graphite reconnect issues
      codahaleMetricsGraphite exclude("com.codahale.metrics", "metrics-core"),
      //dropWizardMetricsGraphite
      dropWizardMetricsJetty,
      socrataHttpCuratorBroker,
      socrataHttpJetty
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
