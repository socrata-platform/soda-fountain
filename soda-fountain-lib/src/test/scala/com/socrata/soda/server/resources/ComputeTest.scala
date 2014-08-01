package com.socrata.soda.server.resources

import com.socrata.soda.server.{TestComputedColumns, DatasetsForTesting}
import com.socrata.soda.server.highlevel.{RowDAO, ExportDAO}
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
    response.getStatus should equal (400)
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

  private def getComputeResponse(dataset: Option[MinimalDatasetRecord] = Some(ds),
                                 col: ColumnName = ds.colName(":computed"),
                                 exportResult: ExportDAO.Result): MockHttpServletResponse = {
    val store = mock[NameAndSchemaStore]
    val exportDAO = mock[ExportDAO]
    val rowDAO = mock[RowDAO]
    val request = new MockHttpServletRequest()
    val response = new MockHttpServletResponse()
    val resource = new Compute(store, exportDAO, rowDAO, TestComputedColumns, NoopEtagObfuscator)

    store.expects('translateResourceName)(*, *).returning(dataset)
    if (dataset.isDefined && dataset.get.columns.filter(_.computationStrategy.isDefined).map(_.fieldName).contains(col))
    {
      exportDAO.expects('export)(*, *, *, *, *, *, *, *, *).returning(exportResult)

      exportResult match {
        case ExportDAO.Success(schema, tag, rows) =>
          rowDAO.expects('upsert)(*, *, *).returning(RowDAO.StreamSuccess(Iterator()))
        case _                                    => // Upsert should have been aborted
      }
    }

    resource.service(dataset.getOrElse(ds).resourceName, col).post(request)(response)
    response
  }
}