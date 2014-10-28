import sbt._

object Dependencies {
  object versions {
    val apacheCurator     = "2.4.2"
    val balboa            = "0.14.0"
    val c3po              = "0.9.2.1"
    val rojomaJson        = "2.4.3"
    val rojomaSimpleArm   = "1.2.0"
    val rojomaSimpleArmV2 = "2.0.0"
    val socrataHttp       = "2.3.4"
    val soqlReference     = "0.3.2"
    val thirdPartyUtils   = "2.5.3"
    val typesafeConfig    = "1.0.2"
  }

  val apacheCuratorDiscovery   = "org.apache.curator" % "curator-x-discovery"   % versions.apacheCurator

  val balboaClient             = "com.socrata"  %% "balboa-client"               % versions.balboa
  val balboaCommon             = "com.socrata"  %% "balboa-common"               % versions.balboa

  val c3po                     = "com.mchange"   % "c3p0"                        % versions.c3po

  val rojomaJson               = "com.rojoma"   %% "rojoma-json"                 % versions.rojomaJson

  val rojomaSimpleArm          = "com.rojoma"   %% "simple-arm"                  % versions.rojomaSimpleArm
  val rojomaSimpleArmV2        = "com.rojoma"   %% "simple-arm-v2"               % versions.rojomaSimpleArmV2

  val socrataHttpClient        = "com.socrata"  %% "socrata-http-client"         % versions.socrataHttp
  val socrataHttpCuratorBroker = "com.socrata"  %% "socrata-http-curator-broker" % versions.socrataHttp
  val socrataHttpJetty         = "com.socrata"  %% "socrata-http-jetty"          % versions.socrataHttp
  val socrataHttpServer        = "com.socrata"  %% "socrata-http-server"         % versions.socrataHttp

  val socrataThirdPartyUtils   = "com.socrata"  %% "socrata-thirdparty-utils"   % versions.thirdPartyUtils

  val soqlAnalyzer             = "com.socrata"  %% "soql-analyzer"               % versions.soqlReference
  val soqlStandaloneParser     = "com.socrata"  %% "soql-standalone-parser"      % versions.soqlReference
  val soqlStdLib               = "com.socrata"  %% "soql-stdlib"                 % versions.soqlReference
  val soqlTypes                = "com.socrata"  %% "soql-types"                  % versions.soqlReference

  val typesafeConfig           = "com.typesafe"  % "config"                      % versions.typesafeConfig

  object Test {
    val apacheCurator          = "org.apache.curator" % "curator-test"                  % versions.apacheCurator   % "test"
    val socrataThirdPartyUtils = "com.socrata"       %% "socrata-thirdparty-test-utils" % versions.thirdPartyUtils % "test"
  }
}
