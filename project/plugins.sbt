resolvers ++= Seq(
    Resolver.url("socrata ivy", url("https://repo.socrata.com/artifactory/ivy-libs-release"))(Resolver.ivyStylePatterns)
)

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.1.0")
addSbtPlugin("com.github.sbt" % "sbt-release" % "1.1.0")

libraryDependencies ++= Seq(
  "com.rojoma" %% "simple-arm-v2" % "2.1.0",
  "com.rojoma" %% "rojoma-json-v3" % "3.9.1"
)
