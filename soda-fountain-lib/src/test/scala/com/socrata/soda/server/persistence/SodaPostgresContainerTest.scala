package com.socrata.soda.server.persistence

import com.socrata.soda.clients.datacoordinator.{DataCoordinatorClient, FeedbackSecondaryManifestClient}
import com.socrata.soda.server.config.ResourceGroupsClientConfig
import com.socrata.soda.server.highlevel.{ColumnSpecUtils, DatasetDAO, DatasetDAOImpl}
import com.socrata.soda.server.id.{ColumnId, DatasetInternalName, ResourceName}
import com.socrata.soda.server.persistence.pg.PostgresStoreImpl
import com.socrata.soda.server.resource_groups.{NoopHttpClient, ResourceGroupsClientFactory}
import com.typesafe.config.ConfigFactory
import org.joda.time.DateTime
import org.scalamock.scalatest.MockFactory

trait SodaPostgresContainerTest extends PostgresContainerTest with MockFactory {
  val resourceGroupsClient = new ResourceGroupsClientFactory(new ResourceGroupsClientConfig(ConfigFactory.empty(),"resource-groups-client"),NoopHttpClient()).client();

  override protected def beforeAll(): Unit = {
    com.socrata.soda.server.persistence.pg.Migration.migrateDb(postgresConnection)
  }

  def generateDataset(humanReadableTestIdentifier: String, columns: Seq[ColumnRecord]): DatasetRecord = {
    val time = System.currentTimeMillis().toString
    val resourceName = new ResourceName(s"${humanReadableTestIdentifier}_$time")
    val datasetId = new DatasetInternalName(s"id.$time")

    new DatasetRecord(
      resourceName,
      datasetId,
      "human name",
      "human description",
      "locale string",
      "mock schema string",
      new ColumnId("mock column id"),
      columns,
      0,
      None,
      new DateTime(0))
  }

  def buildNameAndSchemaStore():NameAndSchemaStore={
    new PostgresStoreImpl(postgresDatasource)
  }

  def buildDatasetDao(
                       dataCoordinatorClient: DataCoordinatorClient = mock[DataCoordinatorClient],
                       feedbackSecondaryManifestClient: FeedbackSecondaryManifestClient = mock[FeedbackSecondaryManifestClient],
                       nameAndSchemaStore: NameAndSchemaStore = mock[NameAndSchemaStore],
                       columnSpecUtils: ColumnSpecUtils = mock[ColumnSpecUtils],
                       instanceForCreate: () => String = mock[() => String]
                     ):DatasetDAO={

    new DatasetDAOImpl(
      dataCoordinatorClient,
      feedbackSecondaryManifestClient,
      resourceGroupsClient,
      nameAndSchemaStore,
      columnSpecUtils,
      instanceForCreate
    )
  }


}
