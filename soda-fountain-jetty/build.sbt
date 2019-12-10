import Dependencies._

libraryDependencies ++= Seq(
  dropWizardMetricsGraphite,
  dropWizardMetricsJetty,
  socrataHttpCuratorBroker,
  socrataHttpJetty
)

mainClass := Some("com.socrata.soda.server.SodaFountainJetty")
