resolvers ++= Seq(
    "socrata maven" at "https://repository-socrata-oss.forge.cloudbees.com/release",
    "Typesafe Repository" at "https://repo.typesafe.com/typesafe/releases/"
)

addSbtPlugin("com.socrata" % "socrata-sbt-plugins" %"1.4.3")

addSbtPlugin("io.spray" % "sbt-revolver" % "0.7.2")

libraryDependencies ++= Seq(
  "com.rojoma" %% "simple-arm" % "1.2.0",
  "com.rojoma" %% "rojoma-json" % "2.4.3"
)
