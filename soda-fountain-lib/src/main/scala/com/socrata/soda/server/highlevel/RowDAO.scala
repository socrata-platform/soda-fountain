package com.socrata.soda.server.highlevel

import com.socrata.soda.server.id.ResourceName
import com.rojoma.json.ast.JValue
import RowDAO._

trait RowDAO {
  def query(dataset: ResourceName, query: String): Result
}

object RowDAO {
  sealed abstract class Result
  case class Success(status: Int, body: JValue) extends Result
  case class NotFound(dataset: ResourceName) extends Result
}
