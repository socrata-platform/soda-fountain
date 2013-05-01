package com.socrata.soda.server.mocks

trait NameAndSchemaStore {

  def getSchemaHash(datasetResourceName: String) : String
}
