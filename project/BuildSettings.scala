import com.socrata.sbtplugins.StylePlugin.StyleKeys._
import sbt.Keys._
import sbt._

object BuildSettings {
  def buildSettings: Seq[Setting[_]] =
    spray.revolver.RevolverPlugin.Revolver.settings ++
    Defaults.itSettings ++
      Seq(
        // TODO: enable coverage minimum
        scoverage.ScoverageSbtPlugin.ScoverageKeys.coverageFailOnMinimum := false,
        // TODO: enable style checks
        styleCheck in Test := {},
        styleCheck in Compile := {},
        scalaVersion := "2.10.4",

        resolvers += "velvia maven" at "http://dl.bintray.com/velvia/maven"
      )

  def projectSettings(assembly: Boolean = false): Seq[Setting[_]] =
    buildSettings
}
