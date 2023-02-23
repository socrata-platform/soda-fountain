import sbt._

object Dependencies {
  object versions {
    val activemq          = "5.13.3"
    val apacheCurator     = "2.4.2"
    val c3p0              = "0.9.5.2"
    val computationStrategies = "0.1.3"
    val dropWizardMetrics = "4.1.2"
    val javaxServletApi   = "2.5"
    val liquibaseCore     = "2.0.0"
    val liquibasePlugin   = "1.9.5.0"
    val metricsScala      = "4.1.1"
    val mortbayJetty      = "6.1.22"
    val msgpack4s         = "0.4.2"
    val postgresql        = "42.2.5"
    val rojomaJson        = "3.14.0"
    val rojomaJsonGrisu   = "1.0.0"
    val rojomaSimpleArm   = "1.2.0"
    val rojomaSimpleArmV2 = "2.1.0"
    val slf4j             = "1.7.33"
    val socrataHttp       = "3.16.0"
    val soqlBrita         = "1.4.1"
    val soqlReference     = "4.11.4"
    val thirdPartyUtils   = "5.0.0"
    val curatorUtils      = "1.2.0"
    val typesafeConfig    = "1.0.2"

    // Test
    val mockito           = "1.10.7"
    val mockServer        = "3.0"
    val scalaCheck        = "1.14.0"
    val scalaMock         = "4.4.0"
    val springTest        = "3.2.10.RELEASE"
    val wiremock          = "1.46"
  }

  val apacheCuratorDiscovery   = "org.apache.curator" % "curator-x-discovery"   % versions.apacheCurator

  val c3p0                     = "com.mchange"      % "c3p0"                        % versions.c3p0

  val computationStrategies     = "com.socrata" %% "computation-strategies"         % versions.computationStrategies

  val dropWizardMetricsGraphite = "io.dropwizard.metrics" % "metrics-graphite"      % versions.dropWizardMetrics
  val dropWizardMetricsJetty    = "io.dropwizard.metrics" % "metrics-jetty9"        % versions.dropWizardMetrics
  val dropWizardMetricsJmx    = "io.dropwizard.metrics" % "metrics-jmx"        % versions.dropWizardMetrics

  val javaxServletApi          = "javax.servlet"    % "servlet-api"                 % versions.javaxServletApi % "provided"

  val liquibaseCore            = "org.liquibase"    % "liquibase-core"              % versions.liquibaseCore

  val liquibasePlugin          = "org.liquibase"    % "liquibase-plugin"            % versions.liquibasePlugin

  val metricsScala             = "nl.grons"        %% "metrics4-scala"               % versions.metricsScala

  val msgpack4s                = "org.velvia"      %% "msgpack4s"                   % versions.msgpack4s

  val mortbayJetty             = "org.mortbay.jetty" % "jetty"                      % versions.mortbayJetty % "container"

  val postgresql               = "org.postgresql"   % "postgresql"                  % versions.postgresql

  val rojomaJsonGrisu          = "com.rojoma"      %% "rojoma-json-v3-grisu"        % versions.rojomaJsonGrisu
  val rojomaJson               = "com.rojoma"      %% "rojoma-json-v3"              % versions.rojomaJson

  val rojomaSimpleArm          = "com.rojoma"      %% "simple-arm"                  % versions.rojomaSimpleArm
  val rojomaSimpleArmV2        = "com.rojoma"      %% "simple-arm-v2"               % versions.rojomaSimpleArmV2

  val slf4j                    = "org.slf4j"        % "slf4j-api"                   % versions.slf4j
  val slf4jLog4j               = "org.slf4j"        % "slf4j-log4j12"               % versions.slf4j

  val socrataHttpClient        = "com.socrata"     %% "socrata-http-client"         % versions.socrataHttp
  val socrataHttpCuratorBroker = "com.socrata"     %% "socrata-http-curator-broker" % versions.socrataHttp exclude("org.slf4j", "slf4j-simple")
  val socrataHttpJetty         = "com.socrata"     %% "socrata-http-jetty"          % versions.socrataHttp
  val socrataHttpServer        = "com.socrata"     %% "socrata-http-server"         % versions.socrataHttp

  val socrataThirdPartyUtils   = "com.socrata"     %% "socrata-thirdparty-utils"    % versions.thirdPartyUtils
  val socrataCuratorUtils      = "com.socrata"     %% "socrata-curator-utils"       % versions.curatorUtils

  val soqlBrita                = "com.socrata"     %% "soql-brita"                  % versions.soqlBrita

  val soqlAnalyzer             = "com.socrata"     %% "soql-analyzer"               % versions.soqlReference
  val soqlPack                 = "com.socrata"     %% "soql-pack"                   % versions.soqlReference
  val soqlStandaloneParser     = "com.socrata"     %% "soql-standalone-parser"      % versions.soqlReference
  val soqlStdLib               = "com.socrata"     %% "soql-stdlib"                 % versions.soqlReference
  val soqlTypes                = "com.socrata"     %% "soql-types"                  % versions.soqlReference

  val typesafeConfig           = "com.typesafe"     % "config"                      % versions.typesafeConfig

  object TestDeps {
    val apacheCurator          = "org.apache.curator"     % "curator-test"          % versions.apacheCurator   % "test"
    val mockito                = "org.mockito"            % "mockito-all"           % versions.mockito         % "test"
    val mockServer             = "org.mock-server"        % "mockserver-netty"      % versions.mockServer      % "test"
    val scalaCheck             = "org.scalacheck"        %% "scalacheck"            % versions.scalaCheck      % "test"
    val scalaMock              = "org.scalamock"         %% "scalamock" % versions.scalaMock % "test"
    val socrataCuratorUtils    = "com.socrata"           %% "socrata-curator-test-utils" % versions.curatorUtils % "test"
    val springTest             = "org.springframework"    % "spring-test"           % versions.springTest      % "test"
    val wiremock               = "com.github.tomakehurst" % "wiremock"              % versions.wiremock        % "test"
    val scalaTest = "org.scalatest" %% "scalatest" % "3.0.8" % "test"
  }
}
