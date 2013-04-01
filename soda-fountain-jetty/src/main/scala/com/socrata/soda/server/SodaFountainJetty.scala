package com.socrata.soda.server

import com.socrata.http.server._
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._

import java.io.FileInputStream
import java.util.Properties
import javax.servlet.http.HttpServletRequest

object SodaFountainJetty {
  def main(args:Array[String]) {
    /*
    val configFileArg = args.find(_.startsWith("--config=")).
      getOrElse(throw new IllegalArgumentException("no config specified - add command line arg for --config=<config file>")).
      substring(9)

    def readProperties = {
      val props = new Properties()
      val propsFile = new FileInputStream(configFileArg)
      props.load(propsFile)
      propsFile.close()
      props
    }
    */

    //val properties = readProperties

    val router = new SodaRouter()
    def service(req: HttpServletRequest): HttpResponse =
      router(req.getMethod, req.getRequestURI.split('/').tail) match {
        case Some(s) =>
          s(req)
        case None =>
          NotFound ~> ContentType("application/json") ~> Content("{\"error\": 404, \"message\": \"Not found.\"}")
      }

    val server = new SocrataServerJetty(service,
      port = 3141)
    server.run
  }
}
