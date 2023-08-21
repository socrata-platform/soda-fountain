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

assembly/assemblyJarName := s"${name.value}-assembly.jar"

assembly/assemblyOutputPath := target.value / (assembly/assemblyJarName).value
