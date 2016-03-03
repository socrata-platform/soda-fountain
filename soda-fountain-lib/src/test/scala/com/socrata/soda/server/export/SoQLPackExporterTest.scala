package com.socrata.soda.server.export

import com.rojoma.simplearm.util._
import com.socrata.soda.server.id.ColumnId
import com.socrata.soda.server.persistence.ColumnRecord
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types._
import com.socrata.soql.SoQLPackIterator
import java.io.{ByteArrayOutputStream, ByteArrayInputStream, DataInputStream}
import javax.servlet.ServletOutputStream
import javax.servlet.http.HttpServletResponse

class SoQLPackExporterTest  extends ExporterTest {

  val points = Seq(
    SoQLPoint.WktRep.unapply("POINT (-122.314822 47.630269)").get,
    SoQLPoint.WktRep.unapply("POINT (-122.319071 47.617296)").get,
    SoQLPoint.WktRep.unapply("POINT (-122.252513 47.555530)").get
  )

  val columns = Seq(
    new ColumnRecord(ColumnId("hym8-ivsj"), ColumnName("name"), SoQLText, "name", "", false, None),
    new ColumnRecord(ColumnId("pw2s-k39x"), ColumnName("location"), SoQLPoint, "location", "", false, None),
    new ColumnRecord(ColumnId("abcd-1234"), ColumnName("visits"), SoQLNumber, "visits", "", false, None)
  )

  def soqlNum(num: Int) = SoQLNumber(new java.math.BigDecimal(num))

  test("Multi row - dataset with single geo column") {

    val rows = Seq[Array[SoQLValue]](
      Array(SoQLText("Volunteer Park"),    SoQLPoint(points(0)), soqlNum(10)),
      Array(SoQLText("Cal Anderson Park"), SoQLPoint(points(1)), soqlNum(15)),
      Array(SoQLText("Seward Park"),       SoQLPoint(points(2)), soqlNum(100))
    )

    val (schema, geomIndex, outRows) = getSoQLPack(columns, "hym8-ivsj", rows, false)

    geomIndex should be (1)
    outRows should have length (3)
    // For some reason cannot compare two Seqs or Lists
    (0 to 2).foreach { row => outRows(row) should equal (rows(row)) }
  }

  test("Single row - dataset with single geo column") {
    val rows = Seq[Array[SoQLValue]](
      Array(SoQLText("Volunteer Park"), SoQLPoint(points(0)), soqlNum(10))
    )

    val (schema, geomIndex, outRows) = getSoQLPack(columns, "hym8-ivsj", rows, true)
    schema.map(_._1) should equal (Seq("name", "location", "visits"))
    outRows(0) should equal (rows(0))
  }

  test("Single row - dataset with null geo column") {
    val rows = Seq[Array[SoQLValue]](
      Array(SoQLText("Volunteer Park"), SoQLNull, soqlNum(10))
    )

    val (schema, geomIndex, outRows) = getSoQLPack(columns, "hym8-ivsj", rows, true)
    geomIndex should equal (1)
    schema.map(_._2) should equal (Seq(SoQLText, SoQLPoint, SoQLNumber))
    outRows(0) should equal (rows(0))
  }


  test("Multi row - dataset with single geo column and some rows with empty geo value") {
    val rows = Seq[Array[SoQLValue]](
      Array(SoQLText("Volunteer Park"),    SoQLPoint(points(0)), soqlNum(10)),
      Array(SoQLText("Cal Anderson Park"), SoQLPoint(points(1)), soqlNum(15)),
      Array(SoQLText("Phantom Park"),      null,                 soqlNum(-2)),
      Array(SoQLText("Seward Park"),       SoQLPoint(points(2)), SoQLNull)
    )

    val (schema, geomIndex, outRows) = getSoQLPack(columns, "hym8-ivsj", rows, false)
    outRows should have length (4)
    outRows(3) should equal (rows(3))
    outRows(2)(1) should equal (SoQLNull)
  }

  private def getSoQLPack(columns: Seq[ColumnRecord],
                         pk: String,
                         rows: Seq[Array[SoQLValue]],
                         singleRow: Boolean): (Seq[(String, SoQLType)], Int, Seq[Array[SoQLValue]]) = {
    for { baos   <- managed(new ByteArrayOutputStream)
          stream <- managed(new ServletOutputStream { def write(b: Int) { baos.write(b) }} ) } yield {
      val mockResponse = mock[HttpServletResponse]
      mockResponse.expects('setContentType)("application/octet-stream")
      mockResponse.expects('getOutputStream)().returning(stream)

      SoQLPackExporter.export(charset, getDCSchema("SoQLPackExporterTest", columns, pk, rows), rows.iterator, singleRow)(mockResponse)

      val dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray))
      val iter = new SoQLPackIterator(dis)
      (iter.schema, iter.geomIndex, iter.toList)
    }
  }
}
