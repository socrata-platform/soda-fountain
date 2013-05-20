package com.socrata.soda.server.persistence

import scala.concurrent.Future

trait NameAndSchemaStore {
  def store(resourceName: String, datasetId: String)
  def translateResourceName( resourceName: String) : Future[Either[String, String]]
}
