import Dependencies._

name := "soda-fountain-message-lib"

libraryDependencies ++= Seq(
  activemqClient,
  awsKinesis,
  rojomaSimpleArm,
  rojomaSimpleArmV2,
  socrataEurybates,
  socrataThirdPartyUtils,
  typesafeConfig
)

disablePlugins(AssemblyPlugin)
