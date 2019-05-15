resolvers ++= Seq(
    Resolver.url("socrata", url("https://repo.socrata.com/artifactory/ivy-libs-release"))(Resolver.ivyStylePatterns)
)

addSbtPlugin("com.socrata" % "socrata-sbt-plugins" %"1.6.8")

addSbtPlugin("io.spray" % "sbt-revolver" % "0.7.2")

libraryDependencies ++= Seq(
  "com.rojoma" %% "simple-arm-v2" % "2.1.0",
  "com.rojoma" %% "rojoma-json-v3" % "3.9.1"
)
