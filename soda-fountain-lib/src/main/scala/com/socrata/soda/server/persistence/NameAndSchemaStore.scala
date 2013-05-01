package com.socrata.soda.server.persistence

abstract class NameAndSchemaStore {

  def getSchemaHash(datasetResourceName: String) : String
}
