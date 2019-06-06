package com.socrata.soda.server.highlevel

import com.socrata.soda.clients.datacoordinator.{FeedbackSecondaryManifestClient, DataCoordinatorClient}
import com.socrata.soda.server.DatasetsForTesting
import com.socrata.soda.server.copy.Latest
import com.socrata.soda.server.highlevel.ColumnDAO._
import com.socrata.soda.server.persistence.NameAndSchemaStore
import com.socrata.soql.environment.ColumnName
import org.scalamock.proxy.ProxyMockFactory
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, FunSuiteLike}
import scala.util.Random

class ColumnDAOSpec extends FunSuiteLike
  with Matchers with MockFactory with ProxyMockFactory with DatasetsForTesting {

  test("Delete nonexistent column") {
    val dataset = TestDatasetWithComputedColumn.dataset
    val toDelete = ColumnName("blah")

    val dc = mock[DataCoordinatorClient]
    val fbm = new FeedbackSecondaryManifestClient(dc, Map.empty)
    val ns = mock[NameAndSchemaStore]
    ns.expects('lookupDataset)(dataset.resourceName, Some(Latest))
      .returning(Some(dataset)).anyNumberOfTimes()
    val col = new ColumnSpecUtils(Random)

    val dao: ColumnDAO = new ColumnDAOImpl(dc, fbm, ns, col)

    val result = dao.deleteColumn("user", dataset.resourceName, None, toDelete, "1234")

    result should be(ColumnNotFound(toDelete))
  }

  test("Delete column with dependency") {
    val dataset = TestDatasetWithComputedColumn.dataset
    val toDelete = dataset.col("source")
    val dependency = dataset.col(":computed")
    val requestId = "1234"

    val dc = mock[DataCoordinatorClient]
    val fbm = new FeedbackSecondaryManifestClient(dc, Map.empty)
    val ns = mock[NameAndSchemaStore]
    ns.expects('lookupDataset)(dataset.resourceName, Some(Latest))
      .returning(Some(dataset)).anyNumberOfTimes()
    val col = new ColumnSpecUtils(Random)

    val dao: ColumnDAO = new ColumnDAOImpl(dc, fbm, ns, col)

    val result = dao.deleteColumn("user", dataset.resourceName, None, toDelete.fieldName, requestId)

    result should be(ColumnHasDependencies(toDelete.fieldName, Seq(dependency.fieldName)))
  }

  test("Delete column") {
    val dataset = TestDatasetWithComputedColumn.dataset
    val toDelete = dataset.col(":computed")
    val requestId = "1234"

    val dc = mock[DataCoordinatorClient]
    val fbm = new FeedbackSecondaryManifestClient(dc, Map.empty)
    val ns = mock[NameAndSchemaStore]
    ns.expects('lookupDataset)(dataset.resourceName, Some(Latest))
      .returning(Some(dataset)).anyNumberOfTimes()
    dc.expects('update)(*, *, *, *, *, *, *)
    val col = new ColumnSpecUtils(Random)

    val dao: ColumnDAO = new ColumnDAOImpl(dc, fbm, ns, col)

    dao.deleteColumn("user", dataset.resourceName, None, toDelete.fieldName, requestId)

    // TODO : How can we pass in/validate the handler function passed to dc.update?
  }
}
