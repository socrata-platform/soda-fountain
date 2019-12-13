import Dependencies._

name := "soda-fountain-message-lib"

libraryDependencies ++= Seq(
  activemqClient,
  rojomaSimpleArm,
  rojomaSimpleArmV2,
  socrataEurybates,
  socrataThirdPartyUtils,
  typesafeConfig
)

disablePlugins(AssemblyPlugin)
