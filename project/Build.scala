import sbt._

object Build extends sbt.Build {
  lazy val build = Project(
    "soda-fountain",
    file(".")
  ).settings(BuildSettings.buildSettings : _*)
   .aggregate(sodaFountainMessageLib, sodaFountainLib, sodaFountainJetty, sodaFountainLibMacros, sodaFountainExternal)

  private def p(name: String, settings: { def settings: Seq[Setting[_]] }, dependencies: ClasspathDep[ProjectReference]*) =
    Project(name, file(name)).settings(settings.settings : _*).configs(IntegrationTest).dependsOn(dependencies: _*)

  val sodaFountainMessageLib = p("soda-fountain-message-lib", SodaFountainMessageLib)
  val sodaFountainLibMacros = p("soda-fountain-lib-macros", SodaFountainLibMacros)
  val sodaFountainLib = p("soda-fountain-lib", SodaFountainLib, sodaFountainLibMacros, sodaFountainMessageLib)
  val sodaFountainJetty = p("soda-fountain-jetty", SodaFountainJetty,  sodaFountainLib)
  val sodaFountainExternal = p("soda-fountain-external", SodaFountainExternal)
}
