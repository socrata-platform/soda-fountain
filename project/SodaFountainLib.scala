
import sbt._
import Keys._

import com.rojoma.json.v3.util.JsonUtil.writeJson
import com.rojoma.simplearm.v2._

import Dependencies._
import Dependencies.Test

object SodaFountainLib {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    resolvers += "Socrata Artifactory" at "https://repo.socrata.com/artifactory/libs-release/",
    resourceGenerators in Compile <+= (resourceManaged in Compile, name in Compile, version in Compile, scalaVersion in Compile) map genVersion,
    libraryDependencies ++= Seq(
      apacheCuratorDiscovery,
      balboaClient,
      c3p0,
      computationStrategies,
      javaxServletApi,
      liquibaseCore,
      liquibasePlugin,
      log4j,
      metricsScala,
      postgresql,
      rojomaSimpleArm,
      rojomaSimpleArmV2,
      scalajHttp,
      socrataHttpClient,
      socrataHttpServer,
      socrataThirdPartyUtils,
      socrataCuratorUtils,
      soqlAnalyzer,
      soqlBrita,
      soqlPack,
      soqlStandaloneParser,
      soqlStdLib exclude ("javax.media", "jai_core"),
      soqlTypes exclude ("javax.media", "jai_core"),
      socrataEurybates,
      activemq,
      typesafeConfig,
      Test.apacheCurator,
      Test.mockito,
      Test.mockServer exclude("ch.qos.logback", "logback-classic"),
      Test.scalaCheck,
      Test.scalaMock,
      Test.socrataCuratorUtils,
      Test.springTest,
      Test.wiremock
    ).map(_.excludeAll(ExclusionRule(organization = "commons-logging")))
  )
  def genVersion(resourceManaged: File, name: String, version: String, scalaVersion: String): Seq[File] = {
    val file = resourceManaged / "soda-fountain-version.json"

    val revision = Process(Seq("git", "describe", "--always", "--dirty", "--long")).!!.split("\n")(0)

    val result = Map(
      "service" -> name,
      "version" -> version,
      "revision" -> revision,
      "scala" -> scalaVersion
    ) ++ sys.env.get("BUILD_TAG").map("build" -> _)

    resourceManaged.mkdirs()
    for {
      stream <- managed(new java.io.FileOutputStream(file))
      w <- managed(new java.io.OutputStreamWriter(stream, "UTF-8"))
    } {
      writeJson(w, result, pretty = true)
      w.write("\n")
    }

    Seq(file)
  }
}
