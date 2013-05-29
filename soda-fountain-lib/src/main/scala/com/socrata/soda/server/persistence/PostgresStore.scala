package com.socrata.soda.server.persistence

import com.socrata.soda.server.services.SodaService
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

trait PostgresStore extends SodaService {

  val store: NameAndSchemaStore = postgres

  object postgres extends NameAndSchemaStore {
    def translateResourceName( resourceName: String) : Future[Either[String, String]] = {
      future {
        ???
      }
    }
    def add(resourceName: String, datasetId: String) = {
      ???
    }
    def remove(resourceName: String) = {
      ???
    }
  }
}


