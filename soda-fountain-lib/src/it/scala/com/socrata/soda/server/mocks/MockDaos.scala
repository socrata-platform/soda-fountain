package com.socrata.soda.server.mocks

import com.socrata.soda.server.services.SodaService
import com.socrata.soda.server.types._
import com.socrata.soda.server.{ColumnDAO, DatasetDAO}
import com.socrata.soda.server.services.ClientRequestExtractor.{UserProvidedColumnSpec, UserProvidedDatasetSpec}
import com.socrata.soql.environment.ColumnName
import scala.util.Random

trait MockDaos extends SodaService {

  val datasetDao : DatasetDAO = mockDatasetDao
  val columnDao : ColumnDAO = mockColumnDao
  val rng : Random = new scala.util.Random

  private object mockDatasetDao extends DatasetDAO {
    import DatasetDAO.Result
    def createDataset(spec: UserProvidedDatasetSpec): Result = ???
    def replaceOrCreateDataset(dataset: ResourceName, spec: UserProvidedDatasetSpec): Result = ???
    def updateDataset(dataset: ResourceName, spec: UserProvidedDatasetSpec): Result = ???
    def deleteDataset(dataset: ResourceName): Result = ???
    def getDataset(dataset: ResourceName): Result = ???
  }

  private object mockColumnDao extends ColumnDAO {
    import ColumnDAO.Result
    def createColumn(dataset: ResourceName, spec: UserProvidedColumnSpec): Result = ???
    def replaceOrCreateColumn(dataset: ResourceName, column: ColumnName, spec: UserProvidedColumnSpec): Result = ???
    def updateColumn(dataset: ResourceName, column: ColumnName, spec: UserProvidedColumnSpec): Result = ???
    def deleteColumn(dataset: ResourceName, column: ColumnName): Result = ???
    def getColumn(dataset: ResourceName, column: ColumnName): Result = ???
  }
}


