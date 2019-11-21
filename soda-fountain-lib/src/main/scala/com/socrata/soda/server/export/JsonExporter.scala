package com.socrata.soda.server.export

import com.rojoma.json.v3.ast.{JArray, JNull, JString}
import com.rojoma.json.v3.io.CompactJsonWriter
import com.rojoma.simplearm.v2._
import com.socrata.http.common.util.AliasedCharset
import com.socrata.http.server.HttpResponse
import com.socrata.soda.server.SodaUtils
import com.socrata.soda.server.highlevel.ExportDAO
import com.socrata.soda.server.wiremodels.{JsonColumnRep, JsonColumnWriteRep}
import com.socrata.soql.types.{SoQLType, SoQLValue}
import java.io.BufferedWriter

import javax.activation.MimeType
import javax.servlet.http.HttpServletResponse
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.soda.message.{MessageProducer, RowsLoadedApiMetricMessage}
import com.socrata.soda.server.id.ResourceName

object JsonExporter extends Exporter {
  val mimeTypeBase = SodaUtils.jsonContentTypeBase
  val mimeType = new MimeType(mimeTypeBase)
  val extension = Some("json")
  val xhRowCount = "X-SODA2-Row-Count"

  def export(charset: AliasedCharset, schema: ExportDAO.CSchema,
             rows: Iterator[Array[SoQLValue]], singleRow: Boolean = false,
             obfuscateId: Boolean = true,
             bom: Boolean = false,
             fuseMap: Map[String, String] = Map.empty)
            (messageProducer: MessageProducer, entityIds: Seq[String], accessType: Option[String]): HttpResponse = {
    val mt = new MimeType(mimeTypeBase)
    mt.setParameter("charset", charset.alias)

    val rowCountHeaders =
      schema.approximateRowCount.fold(NoOp) { rc => Header(xhRowCount, rc.toString) }

    exporterHeaders(schema) ~> rowCountHeaders ~> Write(mt) { rawWriter =>
      using(new BufferedWriter(rawWriter, 65536)) { w =>
        class Processor {
          val writer = w
          val jsonWriter = new CompactJsonWriter(writer)
          val names: Array[String] = schema.schema.map { ci => JString(ci.fieldName.name).toString }.toArray
          val jsonColumnReps = if (obfuscateId) JsonColumnRep.forClientType
                               else JsonColumnRep.forClientTypeClearId
          val reps: Array[JsonColumnWriteRep] = schema.schema.map { ci => jsonColumnReps(ci.typ) }.toArray

          def writeJsonRow(row: Array[SoQLValue]) {
            writer.write('{')
            var didOne = false
            var i = 0
            while(i != row.length) {
              val jsonized = reps(i).toJValue(row(i))
              if(JNull != jsonized) {
                if(didOne) writer.write(',')
                else didOne = true
                writer.write(names(i))
                writer.write(':')
                jsonWriter.write(reps(i).toJValue(row(i)))
              }
              i += 1
            }
            writer.write('}')
          }

          def go(rows: Iterator[Array[SoQLValue]]) {
            var rowsCount = 0
            if(!singleRow) writer.write('[')
            if(rows.hasNext) {
              writeJsonRow(rows.next())
              if(singleRow && rows.hasNext) throw new Exception("Expect to get exactly one row but got more.")
              rowsCount += 1
            }
            while(rows.hasNext) {
              writer.write("\n,")
              writeJsonRow(rows.next())
              rowsCount += 1
            }
            if(!singleRow) writer.write("]\n")
            else writer.write("\n")
            entityIds.foreach(id => messageProducer.send(RowsLoadedApiMetricMessage(id, rowsCount, accessType)))
          }
        }
        val processor = new Processor
        processor.go(rows)
      }
    }
  }

  override protected def maybeSoda2FieldsHeader(schema: ExportDAO.CSchema): HttpServletResponse => Unit = {
    writeSoda2FieldsHeader(schema)
  }
}
