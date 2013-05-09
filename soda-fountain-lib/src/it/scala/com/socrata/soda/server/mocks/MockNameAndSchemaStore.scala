package com.socrata.soda.server.mocks

import com.socrata.soda.server.services.SodaService
import com.socrata.soda.server.persistence.NameAndSchemaStore

trait MockNameAndSchemaStore extends SodaService {
  val store: NameAndSchemaStore = mock

  private object mock extends NameAndSchemaStore {
    val names = new scala.collection.mutable.HashMap[String, String]

    def translateResourceName( resourceName: String) : Option[String] = ???

    def store(resourceName: String, datasetId: String) = names.put(resourceName, datasetId)
  }
}