import sbt._
import Keys._

import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin.MergeStrategy
import sbtassembly.AssemblyUtils._

object SodaFountainExternal {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++
    Seq(
      libraryDependencies ++= Seq(
        "com.rojoma"               %% "rojoma-json"                   % "2.4.3",
        "com.rojoma"               %% "simple-arm"                    % "1.2.0",
        "com.socrata"              %% "socrata-http-client"           % "2.3.4",
        "com.socrata"              %% "socrata-thirdparty-utils"      % "2.5.2",
        "com.typesafe"              % "config"                        % "1.0.2",
        "org.apache.curator"        % "curator-x-discovery"           % "2.4.2"
          exclude("org.slf4j", "slf4j-log4j12")
          exclude("log4j", "log4j"),
        "com.github.tomakehurst"    % "wiremock"                      % "1.46"    % "test",
        "com.socrata"              %% "socrata-thirdparty-test-utils" % "2.5.2"   % "test",
        "org.apache.curator"        % "curator-test"                  % "2.4.2"   % "test",
        "org.scalatest"            %% "scalatest"                     % "2.2.0"   % "test"
      ))
}
