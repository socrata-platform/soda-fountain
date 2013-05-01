package com.socrata.soda.server.mocks

import com.socrata.soda.server.services.SodaService

trait MockNameAndSchemaStore extends SodaService {
  val store: NameAndSchemaStore = mock

  private object mock extends NameAndSchemaStore {
    def getSchemaHash(datasetResourceName: String) : String = "mockSchemaHash"
  }
}