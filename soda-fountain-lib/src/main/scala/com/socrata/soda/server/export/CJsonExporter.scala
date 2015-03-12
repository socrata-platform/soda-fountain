package com.socrata.soda.server.export

import com.rojoma.json.v3.ast.{JString, JNumber}
import com.rojoma.json.v3.codec._
import com.rojoma.json.v3.conversions._
import com.rojoma.json.v3.io.CompactJsonWriter
import com.rojoma.simplearm.util._
import com.socrata.http.common.util.AliasedCharset
import com.socrata.soda.server.highlevel.ExportDAO
import com.socrata.soda.server.util.AdditionalJsonCodecs._
import com.socrata.soda.server.wiremodels.{JsonColumnRep, JsonColumnWriteRep}
import com.socrata.soql.types.SoQLValue
import java.io.BufferedWriter
import javax.activation.MimeType
import javax.servlet.http.HttpServletResponse

object CJsonExporter extends Exporter {
  val mimeTypeBase = "application/json+x-socrata-cjson"
  val mimeType = new MimeType(mimeTypeBase)
  val extension = Some("cjson")

  def export(resp: HttpServletResponse, charset: AliasedCharset, schema: ExportDAO.CSchema, rows: Iterator[Array[SoQLValue]], singleRow: Boolean = false) {
    val mt = new MimeType(mimeTypeBase)
    mt.setParameter("charset", charset.alias)
    resp.setContentType(mt.toString)
    for {
      rawWriter <- managed(resp.getWriter)
      w <- managed(new BufferedWriter(rawWriter, 65536))
    } yield {
      val jw = new CompactJsonWriter(w)
      val schemaOrdering = schema.schema.zipWithIndex.sortBy(_._1.fieldName).map(_._2).toArray
      w.write("""[{""")
      schema.approximateRowCount.foreach { count =>
        w.write(s""""approximate_row_count":${JNumber(count)}""")
        w.write("\n ,")
      }
      w.write(s""""locale":${JString(schema.locale)}""")
      w.write('\n')
      schema.pk.foreach { pk =>
        w.write(s""" ,"pk":${JString(pk.name)}""")
        w.write('\n')
      }
      schema.rowCount.foreach { count =>
        w.write(s""" ,"row_count":${JNumber(count)}""")
        w.write('\n')
      }
      w.write(s""" ,"schema":[""")
      var didOne = false
      for(i <- 0 until schemaOrdering.length) {
        if(didOne) w.write(',')
        else didOne = true
        val ci = schema.schema(schemaOrdering(i))
        w.write(s"""{"c":${JString(ci.fieldName.name)},"t":${JsonEncode.toJValue(ci.typ)}}""")
      }
      w.write("]\n }\n")

      // end of header

      val reps: Array[JsonColumnWriteRep] = schema.schema.map { ci => JsonColumnRep.forClientType(ci.typ) }.toArray
      for(row <- rows) {
        w.write(",[")
        if(row.length > 0) {
          jw.write(reps(schemaOrdering(0)).toJValue(row(schemaOrdering(0))))
          var i = 1
          while(i < row.length) {
            w.write(',')
            jw.write(reps(schemaOrdering(i)).toJValue(row(schemaOrdering(i))))
            i += 1
          }
        }
        w.write("]\n")
      }
      w.write("]\n")
    }
  }
}