package com.socrata.soda.server.highlevel

import com.rojoma.json.ast.{JValue, JObject}
import com.socrata.soda.server.persistence._
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types._

/**
 *  Defines a handler capable of computing one type of computed column
 */
trait ComputationHandler {
  // The type of computation this handler handles.  Should be unique.
  val computationType: String

  /**
   * Handles the actual computation.  Should be lazy if possible, otherwise will introduce
   * significant latency.  IE, try not to convert the Iterator to a regular Seq or Array.
   *
   * @param sourceIt an Iterator of source rows, each row is a JValue (actually an JObject
   *                 of key-value pairs, where the key is the column name).  Anything other
   *                 than a JObject is not for upserts and can be ignored.
   * @param column a ColumnRecord describing the computation and parameters
   * @return an Iterator[JValue] for the output rows.  The JValue must be a JObject containing
   *        a key matching column.name with the computed value.
   */
  def compute(sourceIt: Iterator[JValue], column: MinimalColumnRecord): Iterator[JValue]
}

object ComputationHandler {
  // TODO: These are repeated from RowDAOImpl.  Let's share these somehow.  Or maybe we just make them private.
  case class UnknownColumnEx(colName: ColumnName) extends Exception
  case class MaltypedDataEx(col: ColumnName, expected: SoQLType, got: JValue) extends Exception
  case class ComputationEx(message: String, underlying: Option[Throwable]) extends Exception
}
