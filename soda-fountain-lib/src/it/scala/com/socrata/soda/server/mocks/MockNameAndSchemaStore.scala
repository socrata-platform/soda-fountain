package com.socrata.soda.server.mocks

import com.socrata.soda.server.services.SodaService

trait MockNameAndSchemaStore extends SodaService {
  val store: NameAndSchemaStore = mock

  private object mock extends NameAndSchemaStore {
    val names = new scala.collection.mutable.HashMap[String, BigDecimal]
    val schemas = new scala.collection.mutable.HashMap[BigDecimal, String]

    def getSchemaHash(datasetResourceName: String) : String = "mockSchemaHash"
    def setSchemaHash(resourceName: String, hash:String) = ???

    def translateResourceName( resourceName: String) : Option[String] = ???

    def store(resourceName: String, id: BigDecimal, schemaHash: String) = ???
  }
}