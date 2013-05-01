package com.socrata.soda.server.persistence

class PostgresNameAndSchemaStore extends NameAndSchemaStore {
  def getSchemaHash(datasetResourceName: String) : String = {
    throw new Error("not yet implemented")
  }
}
