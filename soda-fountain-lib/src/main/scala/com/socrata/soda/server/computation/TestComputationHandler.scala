package com.socrata.soda.server.computation

import com.socrata.soda.server.highlevel.RowDataTranslator
import com.socrata.soda.server.highlevel.RowDataTranslator.{DeleteAsCJson, UpsertAsSoQL}
import com.socrata.soda.server.persistence.MinimalColumnRecord
import com.socrata.soql.types.SoQLText

class TestComputationHandler extends ComputationHandler {
  def compute(sourceIt: Iterator[RowDataTranslator.Success], column: MinimalColumnRecord): Iterator[RowDataTranslator.Success] = {
    require(column.computationStrategy.isDefined, "Computation strategy not defined")
    sourceIt.map {
      case UpsertAsSoQL(rowData) => UpsertAsSoQL(rowData + (column.fieldName.name -> SoQLText("foo")))
      case d: DeleteAsCJson      => d
    }.toIterator
  }
}