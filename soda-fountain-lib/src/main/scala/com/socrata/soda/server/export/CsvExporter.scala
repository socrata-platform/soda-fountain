package com.socrata.soda.server.export

import com.rojoma.simplearm.util._
import com.socrata.http.common.util.AliasedCharset
import com.socrata.http.server.HttpResponse
import com.socrata.soda.server.highlevel.ExportDAO
import com.socrata.soda.server.wiremodels.{JsonColumnRep, CsvColumnRep, CsvColumnWriteRep}
import com.socrata.soql.types.SoQLValue
import java.io.BufferedWriter
import javax.activation.MimeType
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._

object CsvExporter extends Exporter {
  val mimeTypeBase = "text/csv"
  val mimeType = new MimeType(mimeTypeBase)
  val extension = Some("csv")

  def export(charset: AliasedCharset, schema: ExportDAO.CSchema,
             rows: Iterator[Array[SoQLValue]], singleRow: Boolean = false,
             obfuscateId: Boolean = true): HttpResponse = {
    Write(new MimeType(mimeTypeBase)) { rawWriter =>
      using(new BufferedWriter(rawWriter, 65536)) { w =>
        val csvColumnReps = if (obfuscateId) CsvColumnRep.forType
                            else CsvColumnRep.forTypeClearId
        class Processor {
          val writer = w
          val reps: Array[CsvColumnWriteRep] = schema.schema.map { f => csvColumnReps(f.typ) }.toArray
          val sb = new java.lang.StringBuilder

          // somewhat surprisingly, writing cells into a stringbuilder and then
          // dumping the result to the writer is slightly faster than writing
          // straight to the writer, even though it's a BufferedWriter.
          def writeCell(cell: String) {
            if(cell != null) {
              sb.setLength(0)
              sb.append('"')
              var j = 0
              while(j != cell.length) {
                val c = cell.charAt(j)
                if(c == '"') sb.append(c)
                sb.append(c)
                j += 1
              }
              sb.append('"')
              writer.write(sb.toString)
            }
          }

          def writeCSVRow(row: Array[String]) {
            if(row.length != 0) {
              writeCell(row(0))
              var i = 1
              while(i != row.length) {
                writer.write(',')
                writeCell(row(i))
                i += 1
              }
            }
            writer.write('\n')
          }

          def convertInto(out: Array[String], row: Array[SoQLValue]) {
            var i = 0
            while(i != out.length) {
              out(i) = reps(i).toString(row(i))
              i += 1
            }
          }

          def go(rows: Iterator[Array[SoQLValue]]) {
            val array = schema.schema.map(_.humanName).toArray
            writeCSVRow(array)
            while(rows.hasNext) {
              convertInto(array, rows.next())
              writeCSVRow(array)
            }
          }
        }
        val processor = new Processor
        processor.go(rows)
      }
    }
  }
}
