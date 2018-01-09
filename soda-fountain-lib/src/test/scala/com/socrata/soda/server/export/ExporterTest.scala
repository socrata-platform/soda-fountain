package com.socrata.soda.server.export

import java.io.OutputStream
import java.nio.charset.StandardCharsets
import javax.servlet.ServletOutputStream

import com.rojoma.json.v3.ast._
import com.socrata.http.common.util.AliasedCharset
import com.socrata.soda.server.DatasetsForTesting
import com.socrata.soda.server.highlevel.CJson.CJsonException
import com.socrata.soda.server.highlevel.ExportDAO.ColumnInfo
import com.socrata.soda.server.highlevel.{CJson, ExportDAO}
import com.socrata.soda.server.persistence.ColumnRecord
import com.socrata.soda.server.wiremodels.JsonColumnRep
import com.socrata.soql.types.{SoQLType, SoQLValue}
import org.joda.time.format.ISODateTimeFormat
import org.scalamock.proxy.ProxyMockFactory
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, FunSuite}

trait ExporterTest extends FunSuite with MockFactory with ProxyMockFactory with Matchers with DatasetsForTesting{

  val charset = AliasedCharset(StandardCharsets.UTF_8, StandardCharsets.UTF_8.name)

  def getDCSchema(humanReadableTestIdentifier: String, columns: Seq[ColumnRecord], pk: String, rows: Seq[Array[SoQLValue]]): ExportDAO.CSchema = {
    val dcInfo = Iterator.single[JValue](getDCSummary(columns, pk, rows.size)) ++ getDCRows(rows)

    try{
      val decoded: CJson.Schema = CJson.decode(dcInfo, JsonColumnRep.forDataCoordinatorType).schema
      val dataset = generateDataset(humanReadableTestIdentifier, columns)

      ExportDAO.CSchema(decoded.approximateRowCount,
        decoded.dataVersion,
        decoded.lastModified.map(time => ISODateTimeFormat.dateTimeParser.parseDateTime(time)),
        decoded.locale,
        decoded.pk.map(dataset.columnsById(_).fieldName),
        decoded.rowCount,
        decoded.schema.map { f =>
          ColumnInfo(dataset.columnsById(f.columnId).id,
                     dataset.columnsById(f.columnId).fieldName,
                     f.typ,
                     None)
        })


    }catch {
      case e: CJsonException => fail("Something got messed up here")
    }
  }

  private def getDCRows(rows: Seq[Array[SoQLValue]]) = {
    val jValues = rows.map(_.map { cell =>
      JsonColumnRep.forDataCoordinatorType(cell.typ).toJValue(cell)
    })
    jValues.map(JArray(_))
  }

  private def getDCSummary(columns: Seq[ColumnRecord], pk: String, rowCount: Int) = {
    val dcRowSchema = columns.map { column =>
      JObject(Map("c" -> JString(column.id.underlying), "t" -> JString(getHumanReadableTypeName(column.typ))))
    }
    JObject(Map("approximate_row_count" -> JNumber(rowCount),
      "locale"                -> JString("en_US"),
      "pk"                    -> JString(pk),
      "schema"                -> JArray(dcRowSchema)))
  }

  private def getHumanReadableTypeName(typ: SoQLType) = SoQLType.typesByName.map(_.swap).get(typ).get.name


}

class FakeServletOutputStream(underlying: OutputStream) extends ServletOutputStream {
  override def write(b: Int): Unit = underlying.write(b)
  override def write(bs: Array[Byte]): Unit = underlying.write(bs)
  override def write(bs: Array[Byte], offset: Int, length: Int): Unit = underlying.write(bs, offset, length)
}
