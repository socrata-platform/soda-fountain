package com.socrata.soda.server

import com.socrata.soda.server.wiremodels.{ColumnSpec, DatasetSpec}
import com.socrata.soda.server.persistence.{ColumnRecordLike, ColumnRecord, DatasetRecord}
import com.socrata.soda.server.id.{ColumnId, DatasetId}
import com.socrata.soda.server.util.schema.SchemaHash

package object highlevel {
  implicit class clrec(val __underlying: ColumnSpec) extends AnyVal {
    def asRecord: ColumnRecord =
      ColumnRecord(
        __underlying.id,
        __underlying.fieldName,
        __underlying.datatype,
        __underlying.name,
        __underlying.description,
        isInconsistencyResolutionGenerated = false
      )
  }

  implicit class clspec(val __underlying: ColumnRecord) extends AnyVal {
    def asSpec: ColumnSpec =
      ColumnSpec(
        __underlying.id,
        __underlying.fieldName,
        __underlying.name,
        __underlying.description,
        __underlying.typ
      )
  }

  implicit class dsrec(val __underlying: DatasetSpec) extends AnyVal {
    def asRecord(datasetId: DatasetId): DatasetRecord = {
      val columns = __underlying.columns.valuesIterator.map(_.asRecord).toSeq
      DatasetRecord(
        __underlying.resourceName,
        datasetId,
        __underlying.name,
        __underlying.description,
        __underlying.locale,
        SchemaHash.computeHash(__underlying.locale, __underlying.columns(__underlying.rowIdentifier).id, columns),
        __underlying.columns(__underlying.rowIdentifier).id,
        columns)
    }
  }
}
