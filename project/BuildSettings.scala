import com.socrata.sbtplugins.StylePlugin.StyleKeys._
import sbt.Keys._
import sbt._
import sbtassembly.AssemblyKeys

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

        resolvers += "velvia maven" at "http://dl.bintray.com/velvia/maven",

        ivyXML :=
          <dependencies>
            <exclude org="com.sun.jmx" module="jmxri"/>         <!--   / log4j          -->
            <exclude org="com.sun.jdmk" module="jmxtools"/>     <!--  <  extra          -->
            <exclude org="javax.jms" module="jms"/>             <!--   \ deps           -->
          </dependencies>
      )

  def projectSettings(assembly: Boolean = false): Seq[Setting[_]] =
    buildSettings ++
    (if (!assembly) Seq(AssemblyKeys.assembly := file(".")) else Nil)
}
