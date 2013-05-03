package com.socrata.soda.server.mocks

import com.socrata.soda.server.services.SodaService

trait PostgresStore extends SodaService {

  val store: NameAndSchemaStore = postgres

  object postgres extends NameAndSchemaStore {
    def getSchemaHash(datasetResourceName: String) : String = ???
  }
}


