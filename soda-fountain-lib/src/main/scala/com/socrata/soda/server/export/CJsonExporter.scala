package com.socrata.soda.server.export

import com.rojoma.json.v3.ast.{JString, JNumber}
import com.rojoma.json.v3.codec._
import com.rojoma.json.v3.conversions._
import com.rojoma.json.v3.io.CompactJsonWriter
import com.rojoma.json.v3.io.CompactJsonWriter
import com.rojoma.simplearm.util._
import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.http.common.util.AliasedCharset
import com.socrata.http.server.HttpResponse
import com.socrata.soda.server.highlevel.ExportDAO
import com.socrata.soda.server.util.AdditionalJsonCodecs._
import com.socrata.soda.server.wiremodels.{JsonColumnRep, JsonColumnWriteRep}
import com.socrata.soql.types.SoQLValue
import java.io.BufferedWriter
import javax.activation.MimeType
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._

object CJsonExporter extends Exporter {
  val mimeTypeBase = "application/json+x-socrata-cjson"
  val mimeType = new MimeType(mimeTypeBase)
  val extension = Some("cjson")

  def export(charset: AliasedCharset, schema: ExportDAO.CSchema,
             rows: Iterator[Array[SoQLValue]], singleRow: Boolean = false,
             obfuscateId: Boolean = true, bom: Boolean = false,
             fuseMap: Map[String, String] = Map.empty): HttpResponse = {
    val mt = new MimeType(mimeTypeBase)

    var ct = 0

    mt.setParameter("charset", charset.alias)
    val jsonColumnReps = if (obfuscateId) JsonColumnRep.forClientType
    else JsonColumnRep.forClientTypeClearId
    val reps: Array[JsonColumnWriteRep] = schema.schema.map { ci => jsonColumnReps(ci.typ) }.toArray

    //val fuser = new LocationFuser(schema, reps, "loc")
    val fusers = fuseMap.foldLeft(Seq.empty[LocationFuser]) { (acc, x) =>
      val (name, typ) = x
      typ match {
        case "location" =>
          acc :+ new LocationFuser(schema, reps, "loc")
        case _ =>
          acc
      }
    }

    val fusedSchema = fusers.foldLeft(schema) { (acc, fuser) =>
      fuser.convertSchema(acc)
    }

    exporterHeaders(fusedSchema) ~> Write(mt) { rawWriter =>
      using(new BufferedWriter(rawWriter, 65536)) { w =>
        val jw = new CompactJsonWriter(w)
        val schemaOrdering = schema.schema.zipWithIndex.sortBy(_._1.fieldName).map(_._2).toArray
        val fusedSchemaOrdering =
          if (fusers.isEmpty) {
            schemaOrdering
          } else {
            fusedSchema.schema.zipWithIndex.sortBy(_._1.fieldName).map(_._2).toArray
          }
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
        for(i <- 0 until fusedSchemaOrdering.length) {
          if(didOne) w.write(',')
          else didOne = true
          val ci = fusedSchema.schema(fusedSchemaOrdering(i))
          w.write(s"""{"c":${JString(ci.fieldName.name)},"t":${JsonEncode.toJValue(ci.typ)}""")
          ci.computationStrategy.foreach { cs =>
            w.write(s""","s":${CompactJsonWriter.toString(cs)}""")
          }
          w.write("}")
        }
        w.write("]\n }\n")
        // end of header

        for(row <- rows) {
          w.write(",[")
          if(row.length > 0) {
            fusers.foreach(fuser => fuser.convert(row))
            ct += 1
            println(ct)
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
}
