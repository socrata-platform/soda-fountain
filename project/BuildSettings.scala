import sbt._
import Keys._

import com.socrata.cloudbeessbt.SocrataCloudbeesSbt

object BuildSettings {
  val buildSettings: Seq[Setting[_]] =
    SocrataCloudbeesSbt.socrataBuildSettings ++
    Defaults.itSettings ++
      Seq(
        scalaVersion := "2.10.4"
      )

  def projectSettings(assembly: Boolean = false): Seq[Setting[_]] =
    buildSettings ++
      SocrataCloudbeesSbt.socrataProjectSettings(assembly)
}
