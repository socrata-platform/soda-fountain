package com.socrata.soda.server.persistence

import com.socrata.soda.server.services.SodaService

trait PostgresStore extends SodaService {

  val store: NameAndSchemaStore = postgres

  object postgres extends NameAndSchemaStore {
    def getSchemaHash(datasetResourceName: String) : String = ???
    def setSchemaHash(resourceName: String, hash:String) = ???

    def translateResourceName( resourceName: String) : Option[String] = ???
    def store(resourceName: String, id: BigDecimal, schemaHash: String) = ???
  }
}


