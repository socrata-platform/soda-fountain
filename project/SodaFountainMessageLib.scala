import Dependencies.{activemqClient, rojomaSimpleArm, rojomaSimpleArmV2, socrataEurybates, socrataThirdPartyUtils, typesafeConfig}
import sbt.Keys.libraryDependencies
import sbt.Setting

object SodaFountainMessageLib {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++
    Seq(
      libraryDependencies ++= Seq(
        activemqClient,
        rojomaSimpleArm,
        rojomaSimpleArmV2,
        socrataEurybates,
        socrataThirdPartyUtils,
        typesafeConfig
      ))
}
