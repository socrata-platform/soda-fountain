resolvers ++= Seq(
    "socrata maven" at "https://repository-socrata-oss.forge.cloudbees.com/release",
    "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
)

addSbtPlugin("com.socrata" % "socrata-cloudbees-sbt" % "1.3.1")

addSbtPlugin("com.earldouglas" % "xsbt-web-plugin" % "0.4.2")

addSbtPlugin("io.spray" % "sbt-revolver" % "0.7.2")

addSbtPlugin("com.typesafe.sbt" % "sbt-license-report" % "1.0.0")

addSbtPlugin("com.codacy" % "sbt-codacy-coverage" % "1.0.0")

libraryDependencies ++= Seq(
  "com.rojoma" %% "simple-arm" % "1.2.0",
  "com.rojoma" %% "rojoma-json" % "2.4.3"
)
