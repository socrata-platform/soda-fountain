package com.socrata.soda.server.persistence

import com.socrata.soda.server.services.SodaService
import scala.concurrent.Future

trait PostgresStore extends SodaService {

  val store: NameAndSchemaStore = postgres

  object postgres extends NameAndSchemaStore {
    def translateResourceName( resourceName: String) : Future[Either[String, String]] = ???
    def store(resourceName: String, datasetId: String) = ???
  }
}


