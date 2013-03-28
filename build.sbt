seq(webSettings :_*)

libraryDependencies ++= Seq(
  "org.mortbay.jetty" % "jetty" % "6.1.22" % "container",
  "javax.servlet" % "servlet-api" % "2.5" % "provided"
)
