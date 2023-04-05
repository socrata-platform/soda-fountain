import scala.sys.process.Process
import com.rojoma.simplearm.v2._
import com.rojoma.json.v3.util.JsonUtil

import Dependencies._

name := "soda-fountain-lib"

Test/fork := true

Compile/resourceGenerators += Def.task {
  genVersion((Compile/resourceManaged).value,
             (Compile/name).value,
             (Compile/version).value,
             (Compile/scalaVersion).value)
}

libraryDependencies ++= Seq(
  apacheCuratorDiscovery,
  c3p0,
  computationStrategies,
  javaxServletApi,
  liquibaseCore,
  liquibasePlugin,
  metricsScala,
  postgresql,
  rojomaSimpleArm,
  rojomaSimpleArmV2,
  slf4j,
  slf4jLog4j,
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
  typesafeConfig,
  TestDeps.apacheCurator,
  TestDeps.mockito,
  TestDeps.mockServer exclude("ch.qos.logback", "logback-classic"),
  TestDeps.scalaCheck,
  TestDeps.scalaMock,
  TestDeps.socrataCuratorUtils,
  TestDeps.springTest,
  TestDeps.wiremock,
  TestDeps.scalaTest,
  TestDeps.testContainers,
  TestDeps.testContainersPostgres
).map(_.excludeAll(ExclusionRule(organization = "commons-logging")))

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
    JsonUtil.writeJson(w, result, pretty = true)
    w.write("\n")
  }

  Seq(file)
}

disablePlugins(AssemblyPlugin)
