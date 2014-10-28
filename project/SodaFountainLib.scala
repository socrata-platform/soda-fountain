import sbt._
import Keys._

import com.rojoma.simplearm.util._
import com.rojoma.json.util.JsonUtil.writeJson
import com.socrata.cloudbeessbt.SocrataCloudbeesSbt.SocrataSbtKeys._

object SodaFountainLib {
  object versions {
    val balboa          = "0.14.0"
    val socrataHttp     = "2.3.4"
    val soqlReference   = "0.3.2"
    val thirdPartyUtils = "2.5.4"
  }

  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    resolvers += "Open Source Geospatial Foundation Repository" at "http://download.osgeo.org/webdav/geotools",
    resourceGenerators in Compile <+= (resourceManaged in Compile, name in Compile, version in Compile, scalaVersion in Compile) map genVersion,
    libraryDependencies ++= Seq(
      "com.mchange"         % "c3p0"                        % "0.9.2.1",
      "com.rojoma"         %% "simple-arm"                  % "[1.2.0,2.0.0)",
      "com.rojoma"         %% "simple-arm-v2"               % "2.0.0",
      "com.socrata"        %% "balboa-common"               % versions.balboa,
      "com.socrata"        %% "balboa-client"               % versions.balboa,
      "com.socrata"        %% "socrata-http-client"         % versions.socrataHttp,
      "com.socrata"        %% "socrata-http-server"         % versions.socrataHttp,
      "com.socrata"        %% "socrata-thirdparty-utils"    % versions.thirdPartyUtils,
      "com.socrata"        %% "soql-analyzer"               % versions.soqlReference,
      "com.socrata"        %% "soql-brita"                  % "[1.2.1,2.0.0)",
      "com.socrata"        %% "soql-standalone-parser"      % versions.soqlReference,
      "com.socrata"        %% "soql-stdlib"                 % versions.soqlReference exclude ("javax.media", "jai_core"),
      "com.socrata"        %% "soql-types"                  % versions.soqlReference,
      "com.typesafe"        % "config"                      % "1.0.2",
      "javax.servlet"       % "servlet-api"                 % "2.5" % "provided",
      "log4j"               % "log4j"                       % "1.2.16",
      "nl.grons"           %% "metrics-scala"               % "3.3.0",
      "org.apache.curator"  % "curator-x-discovery"         % "2.4.2",
      "org.liquibase"       % "liquibase-core"              % "2.0.0",
      "org.liquibase"       % "liquibase-plugin"            % "1.9.5.0",
      "org.scalaj"         %% "scalaj-http"                 % "0.3.15",
      "postgresql"          % "postgresql"                  % "9.1-901-1.jdbc4",
      "org.scalacheck"     %% "scalacheck"                  % "1.10.0"  % "test,it",
      "org.scalatest"      %% "scalatest"                   % "2.2.0"   % "test,it",
      "org.scalamock"      %% "scalamock-scalatest-support" % "3.1.RC1" % "test",
      "org.mockito"         % "mockito-all"                 % "1.10.7"  % "test",
      "org.apache.curator"  % "curator-test"                % "2.4.2"   % "test",
      "org.springframework" % "spring-test"                 % "3.2.10.RELEASE" % "test",
      "com.socrata"        %% "socrata-thirdparty-test-utils" % versions.thirdPartyUtils % "test",
      "org.mock-server"     % "mockserver-netty"            % "3.0"     % "test"
          exclude("ch.qos.logback", "logback-classic")
    )
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
