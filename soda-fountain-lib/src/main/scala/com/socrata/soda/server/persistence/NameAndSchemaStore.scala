package com.socrata.soda.server.persistence

import scala.concurrent.Future

trait NameAndSchemaStore {
  def add(resourceName: String, datasetId: String) : Future[Unit]
  def remove(resourceName: String)                 : Future[Unit]
  def translateResourceName( resourceName: String) : Future[Either[String, String]]
}
