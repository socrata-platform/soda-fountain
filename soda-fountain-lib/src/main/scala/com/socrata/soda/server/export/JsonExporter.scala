package com.socrata.soda.server.export

import com.rojoma.json.v3.ast.{JNull, JString, JArray}
import com.rojoma.json.v3.io.CompactJsonWriter
import com.rojoma.simplearm.util._
import com.socrata.http.common.util.AliasedCharset
import com.socrata.http.server.HttpResponse
import com.socrata.soda.server.SodaUtils
import com.socrata.soda.server.highlevel.ExportDAO
import com.socrata.soda.server.wiremodels.{JsonColumnRep, JsonColumnWriteRep}
import com.socrata.soql.types.{SoQLType, SoQLValue}
import java.io.BufferedWriter
import javax.activation.MimeType
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._

object JsonExporter extends Exporter {
  val mimeTypeBase = SodaUtils.jsonContentTypeBase
  val mimeType = new MimeType(mimeTypeBase)
  val extension = Some("json")
  val xhRowCount = "X-SODA2-Row-Count"
  val xhFields = "X-SODA2-Fields"
  val xhTypes = "X-SODA2-Types"
  val xhDeprecation = "X-SODA2-Warning"
  val xhLimit = 5000 // We have a 6k header size limit

  def export(charset: AliasedCharset, schema: ExportDAO.CSchema,
             rows: Iterator[Array[SoQLValue]], singleRow: Boolean = false,
             obfuscateId: Boolean = true): HttpResponse = {
    val mt = new MimeType(mimeTypeBase)
    mt.setParameter("charset", charset.alias)

    val soda2Fields = CompactJsonWriter.toString(JArray(schema.schema.map(ci => JString(ci.fieldName.name))))
    val soda2Types = CompactJsonWriter.toString(JArray(schema.schema.map(ci => JString(ci.typ.name.name))))
    val rowCountHeaders =
      schema.approximateRowCount.fold(NoOp) { rc => Header(xhRowCount, rc.toString) }
    val deprecatedSodaHeaders =
      if(soda2Fields.length + soda2Types.length < xhLimit) {
        Header(xhFields, soda2Fields) ~>
          Header(xhTypes, soda2Types) ~>
          Header(xhDeprecation, "X-SODA2-Fields and X-SODA2-Types are deprecated.  Use the c-json output format if you require this information.")
      } else {
        Header(xhDeprecation, "X-SODA2-Fields and X-SODA2-Types are deprecated and have been suppressed for being too large.  Use the c-json output format if you require this information.")
      }

    exporterHeaders(schema) ~> rowCountHeaders ~> deprecatedSodaHeaders ~> Write(mt) { rawWriter =>
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
            if(!singleRow) writer.write('[')
            if(rows.hasNext) {
              writeJsonRow(rows.next())
              if(singleRow && rows.hasNext) throw new Exception("Expect to get exactly one row but got more.")
            }
            while(rows.hasNext) {
              writer.write("\n,")
              writeJsonRow(rows.next())
            }
            if(!singleRow) writer.write("]\n")
            else writer.write("\n")
          }
        }
        val processor = new Processor
        processor.go(rows)
      }
    }
  }
}
