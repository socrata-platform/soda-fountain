package com.socrata.soda.server.metrics

import com.rojoma.json.ast.JString

import com.socrata.http.server.util.Precondition
import com.socrata.http.server.routing.OptionallyTypedPathComponent

import com.socrata.soda.clients.datacoordinator.RowUpdate
import com.socrata.soda.server.highlevel.ExportDAO.CSchema
import com.socrata.soda.server.highlevel.RowDAO
import com.socrata.soda.server.highlevel.RowDAO.{UpsertResult, Result}
import com.socrata.soda.server.id.{RowSpecifier, ResourceName}
import com.socrata.soda.server.metrics.Metrics._
import com.socrata.soda.server.metrics.TestDatasets._
import com.socrata.soda.server.persistence.{NameAndSchemaStore, MinimalDatasetRecord, ColumnRecord}
import com.socrata.soda.server.resources.Resource
import com.socrata.soda.server.util.NoopEtagObfuscator

import com.socrata.soql.types.{SoQLValue, SoQLType}

import org.joda.time.DateTime
import org.scalatest.FunSuite
import org.scalamock.scalatest.MockFactory
import org.springframework.mock.web.{MockHttpServletRequest, MockHttpServletResponse}
import com.socrata.soda.server.computation.ComputedColumns

/**
 * Metric scenarios which are common between multi-row queries and single-row operations
 */
trait QueryMetricTestBase extends FunSuite with MockFactory {
  val domainIdHeader = "X-SODA2-Domain-ID"
  val testDomainId = "1"

  test("Querying a cached dataset records a cache hit") {
    mockDatasetQuery(
      CacheHit,
      mockMetricProvider(testDomainId, Seq(QueryCacheHit)),
      Map(domainIdHeader -> testDomainId)
    )
  }

  test("Precondition failure records a user error") {
    mockDatasetQuery(
      PreconditionFailedNoMatch,
      mockMetricProvider(testDomainId, Seq(QueryErrorUser)),
      Map(domainIdHeader -> testDomainId)
    )
  }

  test("Querying a nonexistant dataset records a user error") {
    mockDatasetQuery(
      MissingDataset,
      mockMetricProvider(testDomainId, Seq(QueryErrorUser)),
      Map(domainIdHeader -> testDomainId)
    )
  }

  test("An unhandled exception records an internal error") {
    intercept[Exception] {
      mockDatasetQuery(
        ThrowUnexpectedException,
        mockMetricProvider(testDomainId, Seq(QueryErrorInternal)),
        Map(domainIdHeader -> testDomainId)
      )
    }
  }

  def mockMetricProvider(domainId: String, expectations: Seq[Metric]): MetricProvider = {
    val mockProvider = mock[MetricProvider]
    expectations.foreach(metric => (mockProvider.add(_: Option[String], _: Metric)(_: (Metric => Unit))).expects(Some(domainId), metric, *))
    mockProvider
  }

  def mockDatasetQuery(dataset: TestDataset, provider: MetricProvider, headers: Map[String, String])
}

/**
 * Metric scenarios specific to single row requests
 */
class SingleRowQueryMetricTest extends QueryMetricTestBase {

  test("Sending an If-Modified-Since request for an uncached dataset records a cache miss") {
    mockDatasetQuery(
      SingleRowCacheMiss,
      mockMetricProvider(testDomainId, Seq(QuerySuccess, QueryCacheMiss)),
      Map(
        domainIdHeader -> testDomainId,
        "If-Modified-Since" -> "Sat, 09 Jun 2007 23:55:38 GMT"
      )
    )
  }

  test("Sending an If-None-Match request for an uncached dataset records a cache miss") {
    mockDatasetQuery(
      SingleRowCacheMiss,
      mockMetricProvider(testDomainId, Seq(QuerySuccess, QueryCacheMiss)),
      Map(
        domainIdHeader -> testDomainId,
        "If-None-Match" -> ""
      )
    )
  }

  test("Successfully querying a single row records a success metric") {
    mockDatasetQuery(
      SingleRowSuccess,
      mockMetricProvider(testDomainId,  Seq(QuerySuccess)),
      Map(domainIdHeader -> testDomainId)
    )
  }

  test("Querying an existing dataset with a bad row ID records a user error") {
    mockDatasetQuery(
      SingleRowMissing,
      mockMetricProvider(testDomainId,  Seq(QueryErrorUser)),
      Map(domainIdHeader -> testDomainId)
    )
  }

  def mockDatasetQuery(dataset: TestDataset, provider: MetricProvider, headers: Map[String, String]) {
    val mockResource = new Resource(new QueryOnlyRowDAO(TestDatasets.datasets), mock[NameAndSchemaStore], NoopEtagObfuscator, 1000, null, provider)
    val mockReq = new MockHttpServletRequest()
    mockReq.setRequestURI(s"http://sodafountain/resource/${dataset.dataset}/some-row-id.json")
    headers.foreach(header => mockReq.addHeader(header._1, header._2))

    mockResource.rowService(dataset.resource, new RowSpecifier("some-row-id")).get(mockReq)(new MockHttpServletResponse())
  }
}

/**
 * Metric scenarios specific to multi row requests (queries)
 */
class MultiRowQueryMetricTest extends QueryMetricTestBase {

  test("A user-caused 'invalid request' records a user error") {
    mockDatasetQuery(
      InvalidUserRequest,
      mockMetricProvider(testDomainId, Seq(QueryErrorUser)),
      Map(domainIdHeader -> testDomainId)
    )
  }

  test("An internally-caused 'invalid request' records an internal error") {
    mockDatasetQuery(
      InvalidInternalRequest,
      mockMetricProvider(testDomainId, Seq(QueryErrorInternal)),
      Map(domainIdHeader -> testDomainId)
    )
  }

  test("Sending an If-Modified-Since request for an uncached dataset records a cache miss") {
    mockDatasetQuery(
      MultiRowCacheMiss,
      mockMetricProvider(testDomainId, Seq(QuerySuccess, QueryCacheMiss)),
      Map(
        domainIdHeader -> testDomainId,
        "If-Modified-Since" -> "Sat, 09 Jun 2007 23:55:38 GMT"
      )
    )
  }

  test("Sending an If-None-Match request for an uncached dataset records a cache miss") {
    mockDatasetQuery(
      MultiRowCacheMiss,
      mockMetricProvider(testDomainId, Seq(QuerySuccess, QueryCacheMiss)),
      Map(
        domainIdHeader -> testDomainId,
        "If-None-Match" -> ""
      )
    )
  }

  test("Successfully querying a dataset records a success metric") {
    mockDatasetQuery(
      MultiRowSuccess,
      mockMetricProvider(testDomainId,  Seq(QuerySuccess)),
      Map(domainIdHeader -> testDomainId)
    )
  }

  def mockDatasetQuery(dataset: TestDataset, provider: MetricProvider, headers: Map[String, String]) {
    val mockResource = new Resource(new QueryOnlyRowDAO(TestDatasets.datasets), mock[NameAndSchemaStore], NoopEtagObfuscator, 1000, null, provider)
    val mockReq = new MockHttpServletRequest()
    mockReq.setRequestURI(s"http://sodafountain/resource/${dataset.dataset}.json")
    headers.foreach(header => mockReq.addHeader(header._1, header._2))

    mockResource.service(OptionallyTypedPathComponent(dataset.resource, None)).get(mockReq)(new MockHttpServletResponse())
  }
}

/**
 * Set of test "datasets" which are actually just wrappers for various RowDAO.Results
 */
private object TestDatasets {
  sealed abstract class TestDataset(val dataset: String, result: => RowDAO.Result) {
    val resource = new ResourceName(dataset)
    def getResult = result
  }

  // Convenience
  def querySuccess = RowDAO.QuerySuccess(Seq.empty, 1, DateTime.now(), new CSchema(None, None, None, "en-us", None, None, Seq.empty), Array(new Array[SoQLValue](0)).iterator)
  def singleRowQuerySuccess = RowDAO.SingleRowQuerySuccess(Seq.empty, 1, DateTime.now(),  new CSchema(Some(1), None, None, "en-us", None, Some(1), Seq.empty), new Array[SoQLValue](0))

  // Shared datasets
  case object MissingDataset extends TestDataset("missing-dataset", RowDAO.DatasetNotFound(new ResourceName("missing-dataset")))
  case object PreconditionFailedNoMatch extends TestDataset("precondition-failed-no-match", RowDAO.PreconditionFailed(Precondition.FailedBecauseNoMatch))
  case object ThrowUnexpectedException extends TestDataset("throw-exception", throw new Exception("Mistakes were made"))
  case object CacheHit extends TestDataset("cache-hit", RowDAO.PreconditionFailed(Precondition.FailedBecauseMatch(Seq.empty)))

  // Datasets for single row tests
  case object SingleRowSuccess extends TestDataset("one-row-dataset", singleRowQuerySuccess)
  case object SingleRowCacheMiss extends TestDataset("one-row-cache-miss", singleRowQuerySuccess)
  case object SingleRowMissing extends TestDataset("missing-row", RowDAO.RowNotFound(new RowSpecifier("missing-row")))

  // Datasets for multi row tests
  case object MultiRowSuccess extends TestDataset("multi-rows-dataset", querySuccess)
  case object MultiRowCacheMiss extends TestDataset("multi-row-cache-miss", querySuccess)
  case object InvalidUserRequest extends TestDataset("invalid-user-request-dataset", RowDAO.InvalidRequest(400, JString("you goofed")))
  case object InvalidInternalRequest extends TestDataset("invalid-internal-request-dataset", RowDAO.InvalidRequest(500, JString("we goofed")))

  def datasets: Set[TestDataset] = Set(
    MultiRowSuccess,
    InvalidUserRequest,
    InvalidInternalRequest,
    CacheHit,
    MissingDataset,
    PreconditionFailedNoMatch,
    ThrowUnexpectedException,
    MultiRowCacheMiss,
    SingleRowCacheMiss,
    SingleRowSuccess,
    SingleRowMissing
  )
}

/**
 * Dummy RowDAO that accepts a collection of TestDatasets and simply returns whatever RowDAO.Result they contain.
 */
private class QueryOnlyRowDAO(testDatasets: Set[TestDataset]) extends RowDAO {
  def query(dataset: ResourceName, precondition: Precondition, ifModifiedSince: Option[DateTime], query: String, rowCount: Option[String], secondaryInstance: Option[String]): Result = {
    testDatasets.find(_.resource == dataset).map(_.getResult).getOrElse(throw new Exception("TestDataset not defined"))
  }
  def getRow(dataset: ResourceName, schemaCheck: (Seq[ColumnRecord]) => Boolean, precondition: Precondition, ifModifiedSince: Option[DateTime], rowId: RowSpecifier, secondaryInstance: Option[String]): Result = {
    query(dataset, precondition, ifModifiedSince, "give me one row!", None, secondaryInstance)
  }
  def upsert(user: String, datasetRecord: MinimalDatasetRecord, data: Iterator[RowUpdate]): UpsertResult = ???
  def replace(user: String, datasetRecord: MinimalDatasetRecord, data: Iterator[RowUpdate]): UpsertResult = ???
  def deleteRow(user: String, dataset: ResourceName, rowId: RowSpecifier): UpsertResult = ???
}
