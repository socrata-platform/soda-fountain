package com.socrata.soda.server.highlevel

import com.socrata.soda.clients.datacoordinator.{DataCoordinatorClient, FeedbackSecondaryManifestClient}
import com.socrata.soda.server.config.ResourceGroupsClientConfig
import com.socrata.soda.server.copy.{Published, Unpublished}
import com.socrata.soda.server.id.ResourceName
import com.socrata.soda.server.persistence.NameAndSchemaStore
import com.socrata.soda.server.resource_groups.ResourceGroupsClientFactory
import com.typesafe.config.ConfigFactory
import org.scalamock.proxy.ProxyMockFactory
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import scala.util.Random

class DatasetDAOSpec extends AnyFunSuiteLike with Matchers with MockFactory with ProxyMockFactory {
  test("current copy is latest published") {
    val dataset = new ResourceName("dataset")
    val expectedCopyNum = Some(42)

    val dc = mock[DataCoordinatorClient]
    val fbm = new FeedbackSecondaryManifestClient(dc, Map.empty)
    val ns = mock[NameAndSchemaStore]
    ns.expects('lookupCopyNumber)(dataset, None).returning(Some(1)).anyNumberOfTimes()
    ns.expects('lookupCopyNumber)(dataset, Some(Published)).returning(expectedCopyNum)
    val col = new ColumnSpecUtils(Random)
    val instance = () => "test"
    val resourceGroupsClient = new ResourceGroupsClientFactory(new ResourceGroupsClientConfig(ConfigFactory.empty(),"resource-groups-client")).client();

    val dao: DatasetDAO = new DatasetDAOImpl(dc, fbm,resourceGroupsClient, ns, col, instance)

    val copynum = dao.getCurrentCopyNum(dataset)

    copynum should be(expectedCopyNum)
  }

  test("current copy is latest unpublished") {
    val dataset = new ResourceName("dataset")
    val expectedCopyNum = Some(42)

    val dc = mock[DataCoordinatorClient]
    val fbm = new FeedbackSecondaryManifestClient(dc, Map.empty)
    val ns = mock[NameAndSchemaStore]
    ns.expects('lookupCopyNumber)(dataset, Some(Published)).returning(None)
    ns.expects('lookupCopyNumber)(dataset, Some(Unpublished)).returning(expectedCopyNum)
    val col = new ColumnSpecUtils(Random)
    val instance = () => "test"
    val resourceGroupsClient = new ResourceGroupsClientFactory(new ResourceGroupsClientConfig(ConfigFactory.empty(),"resource-groups-client")).client();


    val dao: DatasetDAO = new DatasetDAOImpl(dc, fbm,resourceGroupsClient, ns, col, instance)

    val copynum = dao.getCurrentCopyNum(dataset)

    copynum should be(expectedCopyNum)
  }
}
