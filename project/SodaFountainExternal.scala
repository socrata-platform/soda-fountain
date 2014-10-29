import sbt._
import Keys._
import Dependencies._
import Dependencies.Test

object SodaFountainExternal {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++
    Seq(
      libraryDependencies ++= Seq(
        apacheCuratorDiscovery exclude("org.slf4j", "slf4j-log4j12") exclude("log4j", "log4j"),
        rojomaJson,
        rojomaSimpleArm,
        socrataHttpClient,
        socrataThirdPartyUtils,
        typesafeConfig,
        Test.apacheCurator,
        Test.scalaTest,
        Test.socrataThirdPartyUtils,
        Test.wiremock
      ))
}
