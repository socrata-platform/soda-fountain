package com.socrata.soda.server

import com.socrata.http.server._
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._

import java.io.FileInputStream
import java.util.Properties
import javax.servlet.http.HttpServletRequest
import com.socrata.soda.server.persistence.PostgresNameAndSchemaStore
import com.socrata.datacoordinator.client.DataCoordinatorClient

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

    val fountain = new SodaFountain(new PostgresNameAndSchemaStore(), new DataCoordinatorClient("http://localhost:12345"))
    val server = new SocrataServerJetty(fountain.router.route, port = 1950)
    server.run
  }
}
