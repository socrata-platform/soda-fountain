package com.socrata.soda.server.mocks

trait NameAndSchemaStore {

  def getSchemaHash(resourceName: String) : String
  def setSchemaHash(resourceName: String, hash:String)

  def store(resourceName: String, id: BigDecimal, schemaHash: String)
  def translateResourceName( resourceName: String) : Option[String]
}
