package com.socrata.soda.server

import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient.ReportMetaData
import com.socrata.soda.server.persistence._
import com.socrata.soda.server.util.schema.SchemaHash
import com.socrata.soda.server.wiremodels.{SourceColumnSpec, ComputationStrategySpec, ColumnSpec, DatasetSpec}
import com.socrata.soql.types.SoQLNull

package object highlevel {
  implicit class clrec(val __underlying: ColumnSpec) extends AnyVal {
    def asRecord: ColumnRecord =
      ColumnRecord(
        __underlying.id,
        __underlying.fieldName,
        __underlying.datatype,
        isInconsistencyResolutionGenerated = false,
        __underlying.computationStrategy.asRecord
      )
  }

  implicit class clspec(val __underlying: ColumnRecord) extends AnyVal {
    def asSpec: ColumnSpec =
      ColumnSpec(
        __underlying.id,
        __underlying.fieldName,
        __underlying.typ,
        __underlying.computationStrategy.asSpec
      )
  }

  implicit class csrec(val __underlying: Option[ComputationStrategySpec]) extends AnyVal {
    def asRecord: Option[ComputationStrategyRecord] = __underlying.map { css =>
      val sourceColumns = css.sourceColumns.map(_.map { col =>
        // Value of last 3 params is never used
        MinimalColumnRecord(col.id, col.fieldName, SoQLNull, false, None)
      })
      ComputationStrategyRecord(css.strategyType, sourceColumns, css.parameters)
    }
  }

  implicit class csspec(val __underlying: Option[ComputationStrategyRecord]) extends AnyVal {
    def asSpec: Option[ComputationStrategySpec] = __underlying.map { csr =>
      val sourceColumns = csr.sourceColumns.map(_.map { col =>
        SourceColumnSpec(col.id, col.fieldName)
      })
      ComputationStrategySpec(csr.strategyType, sourceColumns, csr.parameters)
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
        __underlying.stage,
        dsMetaData.lastModified)
    }
  }

  implicit class dsspec(val __underlying: DatasetRecord) extends AnyVal {
    def asSpec: DatasetSpec = {
      DatasetSpec(
        __underlying.resourceName,
        __underlying.systemId.underlying,
        __underlying.name,
        __underlying.description,
        __underlying.columnsById(__underlying.primaryKey).fieldName,
        __underlying.locale,
        __underlying.stage,
        __underlying.columnsByName.mapValues { cr =>
          ColumnSpec(cr.id, cr.fieldName, cr.typ, cr.computationStrategy.asSpec)
        })
    }
  }
}
