import sbt._
import Keys._

import com.socrata.cloudbeessbt.SocrataCloudbeesSbt

object BuildSettings {
  val buildSettings: Seq[Setting[_]] =
    SocrataCloudbeesSbt.socrataBuildSettings ++
      Seq(
        version := "0.0.16-SNAPSHOT",
        scalaVersion := "2.10.2"
      )

  def projectSettings(assembly: Boolean = false): Seq[Setting[_]] =
    buildSettings ++
      SocrataCloudbeesSbt.socrataProjectSettings(assembly)
}
