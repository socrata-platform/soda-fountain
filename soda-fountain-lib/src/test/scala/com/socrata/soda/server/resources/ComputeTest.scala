package com.socrata.soda.server.resources

import com.rojoma.json.ast.{JValue, JString, JObject}
import com.socrata.http.common.AuxiliaryData
import com.socrata.soda.server.computation.ComputedColumns
import com.socrata.soda.server.highlevel.{RowDAO, ExportDAO}
import com.socrata.soda.server.id.{ColumnId, DatasetId, ResourceName}
import com.socrata.soda.server.persistence._
import com.socrata.soda.server.util.NoopEtagObfuscator
import com.socrata.soda.server.wiremodels.ComputationStrategyType
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types._
import com.socrata.thirdparty.curator.CuratorServiceIntegration
import com.typesafe.config.ConfigFactory
import org.joda.time.DateTime
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{Matchers, FunSuite}
import org.springframework.mock.web.{MockHttpServletRequest, MockHttpServletResponse}

class ComputeTest extends FunSuite with Matchers with MockFactory with CuratorServiceIntegration {

  test("Dataset doesn't exist") {
    val response = getComputeResponse(ds = None,
                                      col = ColumnName("whatever"),
                                      exportResult = null)
    response.getStatus should equal (404)
  }

  test("Column doesn't exist") {
    val response = getComputeResponse(ds = Some(TestData.dataset),
                                      col = ColumnName("mumbo_jumbo"),
                                      exportResult = null)
    response.getStatus should equal (400)
  }

  test("Column exists but not a computed column") {
    val response = getComputeResponse(ds = Some(TestData.dataset),
                                      col = TestData.sourceColumn.fieldName,
                                      exportResult = null)
    response.getStatus should equal (400)
  }
  
  test("Success") {
    val response = getComputeResponse(ds = Some(TestData.dataset),
                                      col = TestData.computedColumn.fieldName,
                                      exportResult = ExportDAO.Success(TestData.dcSchema, None, TestData.dcRows))
    response.getStatus should equal(200)
  }

  private def getComputeResponse(ds: Option[MinimalDatasetRecord] = Some(TestData.dataset),
                                 col: ColumnName = TestData.computedColumn.fieldName,
                                 exportResult: ExportDAO.Result): MockHttpServletResponse = {
    val store = mock[NameAndSchemaStore]
    val exportDAO = mock[ExportDAO]
    val rowDAO = mock[RowDAO]
    val cc = new ComputedColumns[AuxiliaryData](ConfigFactory.load(), discovery)
    val request = new MockHttpServletRequest()
    val response = new MockHttpServletResponse()
    val resource = new Compute(store, exportDAO, rowDAO, cc, NoopEtagObfuscator)

    store.expects('translateResourceName)(*).returning(ds)
    if (ds.isDefined && ds.get.columns.filter(_.computationStrategy.isDefined).map(_.fieldName).contains(col))
    {
      exportDAO.expects('export)(*, *, *, *, *, *, *, *, *).returning(exportResult)

      exportResult match {
        case ExportDAO.Success(schema, tag, rows) =>
          rowDAO.expects('upsert)(*, *, *).returning(RowDAO.StreamSuccess(Iterator()))
        case _                                    => // Upsert should have been aborted
      }
    }

    resource.service(ds.getOrElse(TestData.dataset).resourceName, col).post(request)(response)
    response
  }
}

object TestData {
  val idColumn = MinimalColumnRecord(
    ColumnId(":id"),
    ColumnName(":id"),
    SoQLID,
    isInconsistencyResolutionGenerated =  false
  )

  val sourceColumn = MinimalColumnRecord(
    ColumnId("src1-2345"),
    ColumnName("source"),
    SoQLText,
    isInconsistencyResolutionGenerated =  false
  )

  val computationStrategy = ComputationStrategyRecord(
    ComputationStrategyType.Test,
    true,
    Some(Seq(sourceColumn.id.underlying)),
    Some(JObject(Map("concat_text" -> JString("fun")))))

  val computedColumn = MinimalColumnRecord(
    ColumnId("comp-1234"),
    ColumnName(":computed"),
    SoQLText,
    isInconsistencyResolutionGenerated =  false,
    Some(computationStrategy)
  )

  val dataset = MinimalDatasetRecord(
    new ResourceName("test_resource"),
    new DatasetId("abcd-1234"),
    "en_US",
    "095c0a28ba0a9a0e58f22bf456fc82d27853c1b9",
    new ColumnId(":id"),
    Seq(idColumn, sourceColumn, computedColumn),
    9,
    DateTime.now
  )

  val dcColumns = dataset.columns.map { col => ExportDAO.ColumnInfo(col.id, col.fieldName, "Human Readable Name", col.typ) }
  val dcSchema = ExportDAO.CSchema(
    Some(3), Some(2), Some(DateTime.now), "en_US", Some(ColumnName(":id")), Some(3), dcColumns)

  val dcRows = Iterator(Array[SoQLValue](SoQLID(1), SoQLText("giraffe")),
                        Array[SoQLValue](SoQLID(2), SoQLText("marmot")),
                        Array[SoQLValue](SoQLID(3), SoQLText("axolotl")))
}
