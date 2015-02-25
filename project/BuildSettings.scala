import sbt._
import Keys._

import scoverage.ScoverageSbtPlugin

object BuildSettings {
  val buildSettings: Seq[Setting[_]] =
    spray.revolver.RevolverPlugin.Revolver.settings ++
    Defaults.itSettings ++
      Seq(
        scalaVersion := "2.10.4"
      )

  def projectSettings(assembly: Boolean = false): Seq[Setting[_]] =
    buildSettings
}
