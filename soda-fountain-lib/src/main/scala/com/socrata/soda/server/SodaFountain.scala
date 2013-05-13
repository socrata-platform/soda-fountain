package com.socrata.soda.server

import com.socrata.soda.server.services._

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
  with RowService
  with ColumnService {


}