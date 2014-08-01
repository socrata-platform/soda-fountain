package com.socrata.soda.server.computation

import com.rojoma.json.ast.{JString, JObject}
import com.socrata.soda.server.computation.ComputationHandler.MaltypedDataEx
import com.socrata.soda.server.highlevel.RowDataTranslator
import com.socrata.soda.server.highlevel.RowDataTranslator.{DeleteAsCJson, UpsertAsSoQL}
import com.socrata.soda.server.persistence.{ComputationStrategyRecord, MinimalColumnRecord}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.SoQLText

/**
 * A simple computation handler to ease testing of computed column scenarios.
 * The column value is computed by taking the text of the source column (which must be of type "text"),
 * and concatenating a specified string to it.
 * To trigger this handler, define a column whose computation strategy looks like this:
 * { "type": "test", "recompute": true, "source_columns": ["my_source"], "parameters": { "concat_text" : "foo" } }
 */
class TestComputationHandler extends ComputationHandler {
  def compute(sourceIt: Iterator[RowDataTranslator.Computable], column: MinimalColumnRecord): Iterator[RowDataTranslator.Computable] = {
    require(column.computationStrategy.isDefined, "Computation strategy not defined")
    val (sourceColumn, concatText) = parseStrategy(column.computationStrategy.get)

    sourceIt.map {
      case UpsertAsSoQL(rowData) =>
        val sourceText = getSourceColumnText(rowData, ColumnName(sourceColumn))
        UpsertAsSoQL(rowData + (column.fieldName.name -> SoQLText(s"$sourceText $concatText")))
      case d: DeleteAsCJson      => d
    }.toIterator
  }

  def parseStrategy(computationStrategy: ComputationStrategyRecord): (String, String) = {
    computationStrategy match {
      case ComputationStrategyRecord(_, _, Some(Seq(sourceCol)), Some(JObject(map))) =>
        require(map.contains("concat_text"), "parameters does not contain 'concat_text'")
        val JString(concatText) = map("concat_text")
        (sourceCol, concatText)
      case x =>  throw new IllegalArgumentException("There must be exactly 1 sourceColumn, and " +
        "parameters must have a key 'concat_text'")
    }
  }

  private def getSourceColumnText(rowmap: SoQLRow, colName: ColumnName): String = {
    rowmap.get(colName.name) match {
      case Some(text: SoQLText) => text.value
      case Some(x)              => throw MaltypedDataEx(colName, SoQLText, x.typ)
      case None                 => ""
    }
  }

  def close() {}
}