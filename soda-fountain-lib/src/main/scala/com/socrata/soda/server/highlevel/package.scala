package com.socrata.soda.server

import com.socrata.soda.server.wiremodels.{ComputationStrategySpec, ColumnSpec, DatasetSpec}
import com.socrata.soda.server.persistence.{ColumnRecordLike, ColumnRecord, ComputationStrategyRecord, DatasetRecord}
import com.socrata.soda.server.id.{ColumnId, DatasetId}
import com.socrata.soda.server.util.schema.SchemaHash
import org.joda.time.DateTime
import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient.ReportMetaData

package object highlevel {
  implicit class clrec(val __underlying: ColumnSpec) extends AnyVal {
    def asRecord: ColumnRecord =
      ColumnRecord(
        __underlying.id,
        __underlying.fieldName,
        __underlying.datatype,
        __underlying.name,
        __underlying.description,
        isInconsistencyResolutionGenerated = false,
        __underlying.computationStrategy.asRecord
      )
  }

  implicit class clspec(val __underlying: ColumnRecord) extends AnyVal {
    def asSpec: ColumnSpec =
      ColumnSpec(
        __underlying.id,
        __underlying.fieldName,
        __underlying.name,
        __underlying.description,
        __underlying.typ,
        __underlying.computationStrategy.asSpec
      )
  }

  implicit class csrec(val __underlying: Option[ComputationStrategySpec]) extends AnyVal {
    def asRecord: Option[ComputationStrategyRecord] = __underlying.map { css =>
      ComputationStrategyRecord(css.strategyType, css.recompute, css.sourceColumns, css.parameters)
    }
  }

  implicit class csspec(val __underlying: Option[ComputationStrategyRecord]) extends AnyVal {
    def asSpec: Option[ComputationStrategySpec] = __underlying.map { csr =>
      ComputationStrategySpec(csr.strategyType, csr.recompute, csr.sourceColumns, csr.parameters)
    }
  }

  implicit class dsrec(val __underlying: DatasetSpec) extends AnyVal {
    def asRecord(dsMetaData: ReportMetaData): DatasetRecord = {
      val columns = __underlying.columns.valuesIterator.map(_.asRecord).toSeq
      DatasetRecord(
        __underlying.resourceName,
        dsMetaData.datasetId,
        __underlying.name,
        __underlying.description,
        __underlying.locale,
        SchemaHash.computeHash(__underlying.locale, __underlying.columns(__underlying.rowIdentifier).id, columns),
        __underlying.columns(__underlying.rowIdentifier).id,
        columns,
        dsMetaData.version,
        dsMetaData.lastModified)
    }
  }
}
