import Dependencies._

libraryDependencies ++= Seq(
  apacheCuratorDiscovery exclude("org.slf4j", "slf4j-log4j12") exclude("log4j", "log4j"),
  rojomaJson,
  rojomaSimpleArm,
  socrataHttpClient,
  socrataThirdPartyUtils,
  socrataCuratorUtils,
  typesafeConfig,
  TestDeps.apacheCurator,
  TestDeps.socrataCuratorUtils,
  TestDeps.wiremock,
  TestDeps.scalaTest
)
