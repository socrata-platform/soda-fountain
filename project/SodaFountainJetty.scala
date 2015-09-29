import sbt._
import Keys._
import Dependencies._
import Dependencies.Test

import com.socrata.sbtplugins.CoreSettingsPlugin.SocrataSbtKeys.dependenciesSnippet

object SodaFountainJetty {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ Seq(
    libraryDependencies ++= Seq(
      // See CORE-3635: use lower version of graphite to work around Graphite reconnect issues
      codahaleMetricsGraphite exclude("com.codahale.metrics", "metrics-core"),
      //dropWizardMetricsGraphite
      dropWizardMetricsJetty,
      socrataCuratorUtils,
      socrataHttpCuratorBroker,
      socrataHttpJetty,
      Test.socrataCuratorUtils
    ),
    dependenciesSnippet :=
      <xml.group>
        <exclude org="commons-logging" module="commons-logging"/>
        <exclude org="commons-logging" module="commons-logging-api"/>
      </xml.group>
    ,
    mainClass := Some("com.socrata.soda.server.SodaFountainJetty")
  )
}
