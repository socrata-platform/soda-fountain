package com.socrata.soda.server.persistence

trait NameAndSchemaStore {
  def store(resourceName: String, datasetId: String)
  def translateResourceName( resourceName: String) : Option[String]
}
