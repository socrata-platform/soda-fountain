package com.socrata.soda.server

import com.socrata.soda.server.wiremodels.{ColumnSpec, DatasetSpec}
import com.socrata.soda.server.persistence.{ColumnRecord, DatasetRecord}
import com.socrata.soda.server.id.DatasetId

package object highlevel {
  implicit class clrec(val __underlying: ColumnSpec) extends AnyVal {
    def asRecord: ColumnRecord =
      ColumnRecord(
        __underlying.id,
        __underlying.fieldName,
        __underlying.name,
        __underlying.description
      )
  }

  implicit class dsrec(val __underlying: DatasetSpec) extends AnyVal {
    def asRecord(datasetId: DatasetId): DatasetRecord =
      DatasetRecord(
        __underlying.resourceName,
        datasetId,
        __underlying.name,
        __underlying.description,
        __underlying.columns.valuesIterator.map(_.asRecord).toSeq)
  }
}
