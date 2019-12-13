import Dependencies._

name := "soda-fountain-jetty"

libraryDependencies ++= Seq(
  dropWizardMetricsGraphite,
  dropWizardMetricsJetty,
  dropWizardMetricsJmx,
  socrataHttpCuratorBroker,
  socrataHttpJetty
)

mainClass := Some("com.socrata.soda.server.SodaFountainJetty")
