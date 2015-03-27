package com.socrata.soda.server.resources

import com.socrata.http.client._
import com.socrata.http.server.HttpRequest
import com.socrata.http.server.HttpRequest.AugmentedHttpServletRequest
import com.socrata.soda.server.config.{SodaFountainConfig, SuggestConfig}
import com.socrata.soda.server.highlevel.{ColumnDAO, DatasetDAO}
import com.socrata.soda.server.id.{ColumnId, DatasetId, ResourceName}
import com.socrata.soda.server.persistence.{ColumnRecord, DatasetRecord}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.SoQLNull
import com.typesafe.config.ConfigFactory
import org.joda.time.DateTime
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{FunSuite, Ignore, Matchers}
import org.springframework.mock.web.{MockHttpServletRequest, MockHttpServletResponse}

class SuggestTest extends FunSuite with Matchers with MockFactory {
  val resourceName = new ResourceName("abcd-1234")
  val expectedDatasetId = "primus.1234"
  val expectedCopyNum = 17L
  val columnName = new ColumnName("some_column_name")
  val expectedColumnId = "abcd-1235"
  val suggestText = "nar"
  val expectedSuggestion = "NARCOTICS"
  val expectedStatusCode = 200

  val datasetRecord = new DatasetRecord(
    resourceName, new DatasetId(expectedDatasetId),
    "", "", "", "", new ColumnId(""), Seq.empty, 0L, None, DateTime.now()
  )
  val columnRecord = new ColumnRecord(
    new ColumnId(expectedColumnId), columnName,
    SoQLNull, "", "", false, None
  )

  def mockSuggest(datasetDao: DatasetDAO = mock[DatasetDAO],
                  columnDao: ColumnDAO = mock[ColumnDAO],
                  httpClient: HttpClient = mock[HttpClient],
                  config: SuggestConfig = new SodaFountainConfig(ConfigFactory.load()).suggest
                   ): Suggest =
    Suggest(datasetDao, columnDao, httpClient, config)

  test("spandex url") {
    mockSuggest().spandexAddress should be("localhost:8042")
  }

  test("translate dataset name to id - found") {
    val d = mock[DatasetDAO]
    d.expects('getDataset)(resourceName, None).returning(DatasetDAO.Found(datasetRecord))

    mockSuggest(datasetDao = d).datasetId(resourceName) should be(Some(expectedDatasetId))
  }
  test("translate dataset name to id - not found") {
    val d = mock[DatasetDAO]
    d.expects('getDataset)(resourceName, None).returning(DatasetDAO.NotFound(resourceName))

    mockSuggest(datasetDao = d).datasetId(resourceName) should be(None)
  }
  test("translate dataset name to id - unknown result") {
    val d = mock[DatasetDAO]
    d.expects('getDataset)(resourceName, None).returning(DatasetDAO.Created(datasetRecord))

    a[Exception] should be thrownBy {
      mockSuggest(datasetDao = d).datasetId(resourceName) should be(None)
    }
  }

  test("get latest copy number - found") {
    val d = mock[DatasetDAO]
    d.expects('getCurrentCopyNum)(resourceName).returning(Some(expectedCopyNum))

    mockSuggest(datasetDao = d).copyNum(resourceName) should be(Some(expectedCopyNum))
  }
  test("get latest copy number - not found") {
    val d = mock[DatasetDAO]
    d.expects('getCurrentCopyNum)(resourceName).returning(None)

    mockSuggest(datasetDao = d).copyNum(resourceName) should be(None)
  }

  test("translate column name to id - found") {
    val c = mock[ColumnDAO]
    c.expects('getColumn)(resourceName, columnName).returning(ColumnDAO.Found(datasetRecord, columnRecord, None))

    mockSuggest(columnDao = c).datacoordinatorColumnId(resourceName, columnName) should be(Some(expectedColumnId))
  }
  test("translate column name to id - not found") {
    val c = mock[ColumnDAO]
    c.expects('getColumn)(resourceName, columnName).returning(ColumnDAO.ColumnNotFound(columnName))

    mockSuggest(columnDao = c).datacoordinatorColumnId(resourceName, columnName) should be(None)
  }
  test("translate column name to id - unknown result") {
    val c = mock[ColumnDAO]
    c.expects('getColumn)(resourceName, columnName).returning(ColumnDAO.Created(columnRecord, None))

    a[Exception] should be thrownBy {

      mockSuggest(columnDao = c).datacoordinatorColumnId(resourceName, columnName) should be(None)
    }
  }

  // TODO: fix this test by correctly mocking http response execute
  ignore("get suggestions - found") {
    val d = mock[DatasetDAO]
    d.expects('getDataset)(resourceName, None).returning(DatasetDAO.Found(datasetRecord))
    d.expects('getCurrentCopyNum)(resourceName).returning(Some(expectedCopyNum))

    val c = mock[ColumnDAO]
    c.expects('getColumn)(resourceName, columnName).returning(ColumnDAO.Found(datasetRecord, columnRecord, None))

    val request = new MockHttpServletRequest()
    val augReq = new AugmentedHttpServletRequest(request)
    val httpReq = mock[HttpRequest]
    httpReq.expects('servletRequest)().anyNumberOfTimes.returning(augReq)

    val h = mock[HttpClient]
    h.expects('execute)(augReq).anyNumberOfTimes.returning(42)

    val suggest = mockSuggest(datasetDao = d, columnDao = c, httpClient = h)

    val response = new MockHttpServletResponse()
    suggest.service(resourceName, columnName, suggestText).get(httpReq)(response)

    response.getStatus should be(expectedStatusCode)
    response.getContentAsString should contain(expectedSuggestion)
  }
}
