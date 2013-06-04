package com.socrata.soda.server.mocks

import com.socrata.soda.server.services.SodaService
import com.socrata.soda.server.persistence.NameAndSchemaStore
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

trait MockNameAndSchemaStore extends SodaService {
  val store: NameAndSchemaStore = mock

  private object mock extends NameAndSchemaStore {
    val names = new scala.collection.mutable.HashMap[String, String]

    def translateResourceName( resourceName: String) : Future[Either[String, String]] = {
      future {
        names.get(resourceName) match {
          case Some(rn) => Right(rn)
          case None => Left("could not find dataset")
        }
      }
    }

    def add(resourceName: String, datasetId: String) = future { names.put(resourceName, datasetId) }
    def remove(resourceName: String) = future { names.remove(resourceName) }
  }
}