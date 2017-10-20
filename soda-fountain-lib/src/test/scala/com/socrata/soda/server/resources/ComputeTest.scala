package com.socrata.soda.server.resources

import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.http.server.HttpRequest
import com.socrata.http.server.HttpRequest.AugmentedHttpServletRequest
import com.socrata.soda.server.DatasetsForTesting
import com.socrata.soda.server.highlevel.ColumnDAO
import com.socrata.soda.server.id.ResourceName
import com.socrata.soda.server.persistence._
import com.socrata.soql.environment.ColumnName
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{Matchers, FunSuite}
import org.springframework.mock.web.{MockHttpServletRequest, MockHttpServletResponse}

class ComputeTest extends FunSuite with Matchers with MockFactory with DatasetsForTesting {
  val dsInfo = TestDatasetWithComputedColumn
  val ds = TestDatasetWithComputedColumn.dataset

  test("Dataset doesn't exist") {
    val response = getComputeResponse(dataset = None,
                                      col = ColumnName("whatever"))
    response.getStatus should equal (404)
  }

  test("Column doesn't exist") {
    val response = getComputeResponse(dataset = Some(ds),
                                      col = ColumnName("mumbo_jumbo"))
    response.getStatus should equal (404)
  }

  test("Column exists but not a computed column") {
    val response = getComputeResponse(dataset = Some(ds),
                                      col = ds.col("source").fieldName)
    response.getStatus should equal (400)
  }

  test("Success") {
    val response = getComputeResponse(dataset = Some(ds),
                                      col = ds.col(":computed").fieldName)
    response.getStatus should equal(200)
  }

  private def getComputeResponse(dataset: Option[DatasetRecord] = Some(ds),
                                 col: ColumnName = ds.colName(":computed")): MockHttpServletResponse = {
    val columnDAO = mock[ColumnDAO]
    val request = new MockHttpServletRequest()
    val response = new MockHttpServletResponse()
    val resource = new Compute(columnDAO)

    val httpReq = mock[HttpRequest]
    val augReq = new AugmentedHttpServletRequest(request)
    httpReq.expects('resourceScope)().returning(new ResourceScope())
    httpReq.expects('servletRequest)().anyNumberOfTimes.returning(augReq)

    val getColumnResponse = dataset match {
      case Some(d) => d.columnsByName.get(col) match {
        case Some(c) => ColumnDAO.Found(d, c, None)
        case None => ColumnDAO.ColumnNotFound(col)
      }
      case None => ColumnDAO.DatasetNotFound(new ResourceName("something"))
    }
    columnDAO.expects('getColumn)(*, *).returning(getColumnResponse)

    resource.service(dataset.getOrElse(ds).resourceName, col).post(httpReq)(response)
    response
  }
}