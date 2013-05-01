package com.socrata.soda.server.persistence

class MockNameAndSchemaStore extends NameAndSchemaStore {
  def getSchemaHash(datasetResourceName: String) : String = "mockSchemaHash"
}
