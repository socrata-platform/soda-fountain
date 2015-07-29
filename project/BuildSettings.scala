import com.socrata.sbtplugins.StylePlugin.StyleKeys._
import sbt.Keys._
import sbt._
import sbtassembly.AssemblyKeys

object BuildSettings {
  def buildSettings: Seq[Setting[_]] =
    spray.revolver.RevolverPlugin.Revolver.settings ++
    Defaults.itSettings ++
      Seq(
        // TODO: enable coverage minimum build failure
        scoverage.ScoverageSbtPlugin.ScoverageKeys.coverageFailOnMinimum := false,
        // TODO: enable scalastyle build failure
        styleFailOnError in Compile := false,
        resolvers += "velvia maven" at "http://dl.bintray.com/velvia/maven"
      )

  def projectSettings(assembly: Boolean = false): Seq[Setting[_]] =
    buildSettings ++
    (if (!assembly) Seq(AssemblyKeys.assembly := file(".")) else Nil)
}
