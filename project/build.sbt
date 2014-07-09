resolvers ++= Seq(
    "socrata maven" at "https://repository-socrata-oss.forge.cloudbees.com/release",
    "DiversIT repo" at "http://repository-diversit.forge.cloudbees.com/release"
)

addSbtPlugin("com.socrata" % "socrata-cloudbees-sbt" % "1.2.3")

addSbtPlugin("com.earldouglas" % "xsbt-web-plugin" % "0.4.2")

libraryDependencies ++= Seq(
  "com.rojoma" %% "simple-arm" % "1.2.0",
  "com.rojoma" %% "rojoma-json" % "2.0.0"
)
