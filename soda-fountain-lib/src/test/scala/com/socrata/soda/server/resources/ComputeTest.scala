package com.socrata.soda.server.resources

import com.socrata.http.server.HttpRequest
import com.socrata.http.server.HttpRequest.AugmentedHttpServletRequest
import com.socrata.soda.server.{TestComputedColumns, DatasetsForTesting}
import com.socrata.soda.server.highlevel.{ColumnDAO, RowDAO, ExportDAO}
import com.socrata.soda.server.id.ResourceName
import com.socrata.soda.server.persistence._
import com.socrata.soda.server.util.NoopEtagObfuscator
import com.socrata.soql.environment.ColumnName
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{Matchers, FunSuite}
import org.springframework.mock.web.{MockHttpServletRequest, MockHttpServletResponse}

class ComputeTest extends FunSuite with Matchers with MockFactory with DatasetsForTesting {
  val dsInfo = TestDatasetWithComputedColumn
  val ds = TestDatasetWithComputedColumn.dataset

  test("Dataset doesn't exist") {
    val response = getComputeResponse(dataset = None,
                                      col = ColumnName("whatever"),
                                      exportResult = null)
    response.getStatus should equal (404)
  }

  test("Column doesn't exist") {
    val response = getComputeResponse(dataset = Some(ds),
                                      col = ColumnName("mumbo_jumbo"),
                                      exportResult = null)
    response.getStatus should equal (404)
  }

  test("Column exists but not a computed column") {
    val response = getComputeResponse(dataset = Some(ds),
                                      col = ds.col("source").fieldName,
                                      exportResult = null)
    response.getStatus should equal (400)
  }

  ignore("Success") {
    // TODO - fix this to work with function parameter in export
    val response = getComputeResponse(dataset = Some(ds),
                                      col = ds.col(":computed").fieldName,
                                      exportResult = ExportDAO.Success(dsInfo.dcSchema, None, dsInfo.dcRows))
    response.getStatus should equal(200)
  }

  private def getComputeResponse(dataset: Option[DatasetRecord] = Some(ds),
                                 col: ColumnName = ds.colName(":computed"),
                                 exportResult: ExportDAO.Result): MockHttpServletResponse = {
    val columnDAO = mock[ColumnDAO]
    val exportDAO = mock[ExportDAO]
    val rowDAO = mock[RowDAO]
    val request = new MockHttpServletRequest()
    val response = new MockHttpServletResponse()
    val resource = new Compute(columnDAO, exportDAO, rowDAO, TestComputedColumns, NoopEtagObfuscator)

    val httpReq = mock[HttpRequest]
    val augReq = new AugmentedHttpServletRequest(request)
    httpReq.expects('servletRequest)().anyNumberOfTimes.returning(augReq)

    val getColumnResponse = dataset match {
      case Some(d) => d.columnsByName.get(col) match {
        case Some(c) => ColumnDAO.Found(d, c, None)
        case None    => ColumnDAO.ColumnNotFound(col)
      }
      case None    => ColumnDAO.DatasetNotFound(new ResourceName("something"))
    }

    columnDAO.expects('getColumn)(*, *).returning(getColumnResponse)
    if (getColumnResponse.isInstanceOf[ColumnDAO.Found] &&
        dataset.get.columnsByName.get(col).get.computationStrategy.isDefined) {
      exportDAO.expects('export)(*, *, *, *, *, *, *, "latest", *).returning(exportResult)

      exportResult match {
        case ExportDAO.Success(schema, tag, rows) =>
          rowDAO.expects('upsert)(*, *, *).returning(RowDAO.StreamSuccess(Iterator()))
        case _                                    => // Upsert should have been aborted
      }
    }

    resource.service(dataset.getOrElse(ds).resourceName, col).post(httpReq)(response)
    response
  }
}