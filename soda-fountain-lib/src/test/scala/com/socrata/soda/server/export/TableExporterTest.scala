package com.socrata.soda.server.export

import javax.servlet.ServletOutputStream
import com.rojoma.simplearm.v2._
import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.conversions._
import com.socrata.soda.server.export.GeoJsonProcessor.InvalidGeoJsonSchema
import com.socrata.soda.server.id.{ColumnId, ResourceName}
import com.socrata.soda.server.persistence.ColumnRecord
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types._
import com.socrata.thirdparty
import java.io.{ByteArrayOutputStream, OutputStream, PrintWriter, StringWriter}

import com.socrata.soda.message.NoOpMessageProducer
import javax.servlet.http.HttpServletResponse
import com.socrata.thirdparty.opencsv.CSVIterator
import com.sun.xml.internal.fastinfoset.util.StringArray
import org.joda.time.format.ISODateTimeFormat

class TableExporterTest extends ExporterTest {

  test("Export a dataset as a CSV") {
    val columns = Seq(
      new ColumnRecord(ColumnId("hym8-ivsj"), ColumnName("name"), SoQLText, false, None),
      new ColumnRecord(ColumnId("pw2s-k39x"), ColumnName("other"), SoQLText, false, None)
    )

    val rows = Seq[Array[SoQLValue]](
      Array(SoQLText("Volunteer Park"), SoQLText("Pratt Park"))
    )
    val csvTable = getTable(columns, "hym8-ivsj", rows, CsvExporter, "text/csv", singleRow = false)
    val expectedCsvTable = "\"name\",\"other\"\n\"Volunteer Park\",\"Pratt Park\"\n"

    csvTable should be(expectedCsvTable)
  }

  test("Export a dataset as TSV") {
    val columns = Seq(
      new ColumnRecord(ColumnId("hym8-ivsj"), ColumnName("name"), SoQLText, false, None),
      new ColumnRecord(ColumnId("pw2s-k39x"), ColumnName("other"), SoQLText, false, None)
    )

    val rows = Seq[Array[SoQLValue]](
      Array(SoQLText("Volunteer Park"), SoQLText("Pratt Park"))
    )
    val expectedTsvTable = "\"name\"\t\"other\"\n\"Volunteer Park\"\t\"Pratt Park\"\n"
    val tsvTable = getTable(columns, "hym8-ivsj", rows, TsvExporter, "text/tab-separated-values", singleRow = false)
    tsvTable should be(expectedTsvTable)
  }

  test("Export a dataset with a comma character as CSV") {
    val columns = Seq(
      new ColumnRecord(ColumnId("hym8-ivsj"), ColumnName("name,"), SoQLText, false, None),
      new ColumnRecord(ColumnId("pw2s-k39x"), ColumnName("other"), SoQLText, false, None)
    )

    val rows = Seq[Array[SoQLValue]](
      Array(SoQLText("Volunteer Park"), SoQLText("Pratt Park"))
    )
    val expectedCsvTable = "\"name,\",\"other\"\n\"Volunteer Park\",\"Pratt Park\"\n"
    val csvTable = getTable(columns, "hym8-ivsj", rows, CsvExporter, "text/csv", singleRow = false)
    csvTable should be(expectedCsvTable)

  }

  test("Export a dataset with a tab character as TSV") {
    val columns = Seq(
      new ColumnRecord(ColumnId("hym8-ivsj"), ColumnName("name\t"), SoQLText, false, None),
      new ColumnRecord(ColumnId("pw2s-k39x"), ColumnName("other"), SoQLText, false, None)
    )

    val rows = Seq[Array[SoQLValue]](
      Array(SoQLText("Volunteer Park"), SoQLText("Pratt Park"))
    )
    val expectedTsvTable = "\"name\t\"\t\"other\"\n\"Volunteer Park\"\t\"Pratt Park\"\n"
    val tsvTable = getTable(columns, "hym8-ivsj", rows, TsvExporter, "text/tab-separated-values", singleRow = false)
    tsvTable should be(expectedTsvTable)
  }

  test("Export a dataset with a line break as CSV") {
    val columns = Seq(
      new ColumnRecord(ColumnId("hym8-ivsj"), ColumnName("name\n"), SoQLText, false, None),
      new ColumnRecord(ColumnId("pw2s-k39x"), ColumnName("other"), SoQLText, false, None)
    )

    val rows = Seq[Array[SoQLValue]](
      Array(SoQLText("Volunteer Park\n"), SoQLText("Pratt Park"))
    )
    val expectedCsvTable = "\"name\n\",\"other\"\n\"Volunteer Park\n\",\"Pratt Park\"\n"
    val csvTable = getTable(columns, "hym8-ivsj", rows, CsvExporter, "text/csv", singleRow = false)
    csvTable should be(expectedCsvTable)

  }

  // an extra double quote in a header or field is handled like this: x" -> "x""
  // and with the extra quotes added in the exporter the value looks like this: x" -> "x"""
  test("Export a dataset with line break as TSV") {
    val columns = Seq(
      new ColumnRecord(ColumnId("hym8-ivsj"), ColumnName("name\n"), SoQLText, false, None),
      new ColumnRecord(ColumnId("pw2s-k39x"), ColumnName("other"), SoQLText, false, None)
    )

    val rows = Seq[Array[SoQLValue]](
      Array(SoQLText("Volunteer Park\n"), SoQLText("Pratt Park"))
    )
    val expectedTsvTable = "\"name\n\"\t\"other\"\n\"Volunteer Park\n\"\t\"Pratt Park\"\n"
    val tsvTable = getTable(columns, "hym8-ivsj", rows, TsvExporter, "text/tab-separated-values", singleRow = false)
    tsvTable should be(expectedTsvTable)
  }

  test("Export a dataset with double quotes as CSV") {
    val columns = Seq(
      new ColumnRecord(ColumnId("hym8-ivsj"), ColumnName("name\""), SoQLText, false, None),
      new ColumnRecord(ColumnId("pw2s-k39x"), ColumnName("other"), SoQLText, false, None)
    )
    val rows = Seq[Array[SoQLValue]](
      Array(SoQLText("Volunteer Park\""), SoQLText("Pratt Park"))
    )
    val expectedCsvTable = "\"name\"\"\",\"other\"\n\"Volunteer Park\"\"\",\"Pratt Park\"\n"
    val csvTable = getTable(columns, "hym8-ivsj", rows, CsvExporter, "text/csv", singleRow = false)
    csvTable should be(expectedCsvTable)
  }

  test("Export a dataset with double quotes as TSV") {
    val columns = Seq(
      new ColumnRecord(ColumnId("hym8-ivsj"), ColumnName("name\""), SoQLText, false, None),
      new ColumnRecord(ColumnId("pw2s-k39x"), ColumnName("other"), SoQLText, false, None)
    )

    val rows = Seq[Array[SoQLValue]](
      Array(SoQLText("Volunteer Park\""), SoQLText("Pratt Park"))
    )
    val expectedTsvTable = "\"name\"\"\"\t\"other\"\n\"Volunteer Park\"\"\"\t\"Pratt Park\"\n"
    val tsvTable = getTable(columns, "hym8-ivsj", rows, TsvExporter, "text/tab-separated-values", singleRow = false)
    tsvTable should be(expectedTsvTable)
  }

  private def getTable(columns: Seq[ColumnRecord],
                       pk: String,
                       rows: Seq[Array[SoQLValue]],
                       tableExporter: TableExporter,
                       contentType: String,
                       singleRow: Boolean): String = {
    for {
      out <- managed(new ByteArrayOutputStream)
      wrapped <- managed(new FakeServletOutputStream(out))
    } {
      val mockResponse = mock[HttpServletResponse]
      mockResponse.expects('setContentType)(s"$contentType; charset=UTF-8")
      mockResponse.expects('getOutputStream)().returning(wrapped)

      tableExporter.export(charset, getDCSchema("TableExporterTest", columns, pk, rows), rows.iterator, singleRow)(NoOpMessageProducer, None)(mockResponse)
      new String(out.toByteArray, "UTF-8")
    }
  }

}
