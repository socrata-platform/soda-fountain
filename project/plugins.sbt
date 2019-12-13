resolvers ++= Seq(
    Resolver.url("socrata ivy", url("https://repo.socrata.com/artifactory/ivy-libs-release"))(Resolver.ivyStylePatterns)
)

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.10")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.12")

libraryDependencies ++= Seq(
  "com.rojoma" %% "simple-arm-v2" % "2.1.0",
  "com.rojoma" %% "rojoma-json-v3" % "3.9.1"
)
