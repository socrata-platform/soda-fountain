package com.socrata.soda.server

import com.socrata.soda.server.mocks._
import com.socrata.datacoordinator.client.DataCoordinatorClient
import com.socrata.soda.server.services.{SodaService, CatalogService, ColumnService, DatasetService}

object SodaFountain {

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
}

abstract class SodaFountain
  extends SodaService
  with DatasetService
  with ColumnService
  with CatalogService {


}