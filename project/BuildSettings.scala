import sbt._
import Keys._

import com.socrata.cloudbeessbt.SocrataCloudbeesSbt
import scoverage.ScoverageSbtPlugin

object BuildSettings {
  val buildSettings: Seq[Setting[_]] =
    SocrataCloudbeesSbt.socrataBuildSettings ++
    spray.revolver.RevolverPlugin.Revolver.settings ++
    Defaults.itSettings ++
      Seq(
        scalaVersion := "2.10.4",
        ScoverageSbtPlugin.ScoverageKeys.coverageHighlighting := false
      )

  def projectSettings(assembly: Boolean = false): Seq[Setting[_]] =
    buildSettings ++
      SocrataCloudbeesSbt.socrataProjectSettings(assembly)
}
