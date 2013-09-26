import sbt._
import Keys._

import com.rojoma.simplearm.util._
import com.rojoma.json.util.JsonUtil.writeJson

object SodaFountainLib {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    resourceGenerators in Compile <+= (resourceManaged in Compile, name in Compile, version in Compile, scalaVersion in Compile) map genVersion,
    libraryDependencies ++= Seq(
      "com.socrata" %% "socrata-thirdparty-utils" % "[2.0.0,3.0.0)",
      "com.socrata" %% "soql-types" % "0.0.16-SNAPSHOT",
      "com.socrata" %% "soql-brita" % "[1.2.1,2.0.0)",
      "javax.servlet" % "servlet-api" % "2.5" % "provided",
      "com.rojoma" %% "simple-arm" % "[1.2.0,2.0.0)",
      "com.socrata" %% "socrata-http-server" % "2.0.0-SNAPSHOT",
      "com.socrata" %% "socrata-http-client" % "2.0.0-SNAPSHOT",
      "com.typesafe" % "config" % "1.0.2",
      "com.netflix.curator" % "curator-x-discovery" % "1.3.3",
      "postgresql" % "postgresql" % "9.1-901-1.jdbc4",
      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test,it",
      "org.scalatest" %% "scalatest" % "1.9.1" % "test,it"
    )
  )

  def genVersion(resourceManaged: File, name: String, version: String, scalaVersion: String): Seq[File] = {
    val file = resourceManaged / "soda-fountain-version.json"

    val revision = Process(Seq("git", "describe", "--always", "--dirty")).!!.split("\n")(0)

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
