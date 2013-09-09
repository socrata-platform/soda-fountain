package com.socrata.soda.server.highlevel

import com.socrata.soda.server.id.ResourceName
import com.rojoma.json.ast.JValue
import RowDAO._

trait RowDAO {
  def query(dataset: ResourceName, query: String): Result
  def upsert[T](dataset: ResourceName, data: Iterator[JValue])(f: UpsertResult => T): T
  def replace[T](dataset: ResourceName, data: Iterator[JValue])(f: UpsertResult => T): T
}

object RowDAO {
  sealed abstract class Result
  sealed trait UpsertResult
  case class Success(status: Int, body: JValue) extends Result
  case class StreamSuccess(report: Iterator[JValue]) extends UpsertResult // TODO: Not JValue
  case class NotFound(dataset: ResourceName) extends Result with UpsertResult
}
