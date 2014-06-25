package com.socrata.soda.server.export

import com.socrata.soda.server.highlevel.ExportDAO
import com.socrata.soql.types.SoQLValue
import javax.activation.MimeType
import javax.servlet.http.HttpServletResponse
import org.geotools.geojson.geom.GeometryJSON
import com.rojoma.simplearm.util._
import com.socrata.http.common.util.AliasedCharset
import scala.Some
import java.io.BufferedWriter
import com.vividsolutions.jts.geom.GeometryCollection

class GeoJsonExporter extends Exporter {
  val mimeTypeBase = "application/vnd.geo+json"
  val mimeType = new MimeType(mimeTypeBase)
  val extension = Some("geojson")

  def export(resp: HttpServletResponse,
             charset: AliasedCharset,
             schema: ExportDAO.CSchema,
             rows: Iterator[Array[SoQLValue]],
             singleRow: Boolean = false) {
    val mt = new MimeType(mimeTypeBase)
    mt.setParameter("charset", charset.alias)
    resp.setContentType(mt.toString)

    // TODO : Return HTTP 406 if the dataset doesn't contain geo columns.
    // This should happen in Resource before retrieving rows,
    // so we aren't pulling a large dataset from secondary
    // only to immediately throw it away.

    for {
      rawWriter <- managed(resp.getWriter)
      w <- managed(new BufferedWriter(rawWriter, 65536))
    } yield {
      class Processor {
        def go(rows: Iterator[Array[SoQLValue]]) {
        }
      }
      val processor = new Processor
      processor.go(rows)
    }
  }
}
