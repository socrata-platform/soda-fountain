package com.socrata.soda.server.persistence

import scala.concurrent.Future

trait NameAndSchemaStore {
  def add(resourceName: String, datasetId: String)
  def remove(resourceName: String)
  def translateResourceName( resourceName: String) : Future[Either[String, String]]
}
