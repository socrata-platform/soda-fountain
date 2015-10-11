package com.socrata.soda.server.export

import com.socrata.soql.SoQLPackIterator
import com.socrata.soql.types.SoQLArray
import com.socrata.soql.types.SoQLBoolean
import com.socrata.soql.types.SoQLDate
import com.socrata.soql.types.SoQLDouble
import com.socrata.soql.types.SoQLFixedTimestamp
import com.socrata.soql.types.SoQLFloatingTimestamp
import com.socrata.soql.types.SoQLID
import com.socrata.soql.types.SoQLJson
import com.socrata.soql.types.SoQLMoney
import com.socrata.soql.types.SoQLNull
import com.socrata.soql.types.SoQLNumber
import com.socrata.soql.types.SoQLObject
import com.socrata.soql.types.SoQLPoint
import com.socrata.soql.types.SoQLText
import com.socrata.soql.types.SoQLTime
import com.socrata.soql.types.SoQLValue
import com.socrata.soql.types.SoQLVersion
import com.socrata.soql.types._
import com.vividsolutions.jts.geom.Polygon
import java.math.{BigDecimal => BD}
import org.joda.time.{DateTime, LocalDateTime}
import org.scalatest.{FunSuite, MustMatchers}

class SoQLPackTest extends FunSuite with MustMatchers {
  val wkt1 = "POINT (47.123456 -122.123456)"
  val point1 = SoQLPoint.WktRep.unapply(wkt1).get

  val schema1 = Seq("id" -> SoQLID,
    "ver" -> SoQLVersion,
    "str" -> SoQLText,
    "bool" -> SoQLBoolean,
    "point" -> SoQLPoint)

  val data1: Seq[Array[SoQLValue]] = Seq(
    Array(SoQLID(1L), SoQLVersion(11L), SoQLText("first"),  SoQLBoolean(true),  SoQLPoint(point1)),
    Array(SoQLID(2L), SoQLVersion(12L), SoQLText("second"), SoQLBoolean(false), SoQLNull)
  )

  val schema2 = Seq("num" -> SoQLNumber,
    "$" -> SoQLMoney,
    "dbl" -> SoQLDouble)

  val data2: Seq[Array[SoQLValue]] = Seq(
    Array(SoQLNumber(new BD(12345678901L)), SoQLMoney(new BD(9999)), SoQLDouble(0.1)),
    Array(SoQLNumber(new BD(-123.456)),     SoQLMoney(new BD(9.99)), SoQLDouble(-99.0))
  )

  def writeThenRead(writer: java.io.OutputStream => Unit)(reader: java.io.DataInputStream => Unit) {
    val baos = new java.io.ByteArrayOutputStream
    try {
      writer(baos)
      val bais = new java.io.ByteArrayInputStream(baos.toByteArray)
      val dis = new java.io.DataInputStream(bais)
      try {
        reader(dis)
      } finally {
        dis.close
        bais.close
      }
    } finally {
      baos.close
    }
  }

  test("Can serialize SoQL rows to and from SoQLPack") {
    writeThenRead { os =>
      val w = new SoQLPackWriter(schema1)
      w.write(os, data1.toIterator)
    } { dis =>
      val r = new SoQLPackIterator(dis)
      r.geomIndex must equal (4)
      r.schema must equal (schema1)
      val outRows = r.toList
      outRows must have length (data1.length)
      outRows(0) must equal (data1(0))
      outRows(1) must equal (data1(1))
    }

    writeThenRead { os =>
      val w = new SoQLPackWriter(schema2)
      w.write(os, data2.toIterator)
    } { dis =>
      val r = new SoQLPackIterator(dis)
      r.geomIndex must equal (-1)
      r.schema must equal (schema2)
      val outRows = r.toList
      outRows must have length (data2.length)
      outRows(0) must equal (data2(0))
      outRows(1) must equal (data2(1))
    }
  }

  val dt1 = DateTime.parse("2015-03-22T12Z")
  val dt2 = DateTime.parse("2015-03-22T12:00:00-08:00")
  val ldt = LocalDateTime.parse("2015-03-22T01:23")
  val date = ldt.toLocalDate
  val time = ldt.toLocalTime

  val schemaDT = Seq("dt" -> SoQLFixedTimestamp,
    "ldt" -> SoQLFloatingTimestamp,
    "date" -> SoQLDate,
    "time" -> SoQLTime)

  val dataDT: Seq[Array[SoQLValue]] = Seq(
    Array(SoQLFixedTimestamp(dt1), SoQLFloatingTimestamp(ldt),              SoQLDate(date), null),
    Array(SoQLFixedTimestamp(dt2), SoQLFloatingTimestamp(ldt.plusHours(1)), SoQLNull, SoQLTime(time))
  )

  test("Can serialize SoQL rows with date time types to and from SoQLPack") {
    writeThenRead { os =>
      val w = new SoQLPackWriter(schemaDT)
      w.write(os, dataDT.toIterator)
    } { dis =>
      val r = new SoQLPackIterator(dis)
      r.geomIndex must equal (-1)
      r.schema must equal (schemaDT)
      val outRows = r.toList
      outRows must have length (dataDT.length)
      // NOTE: just comparing raw DateTime objects doesn't work because even though the timezones
      // have the same offset, they render differently [-07:00] vs [America/Los_Angeles].
      outRows(0).mkString(",") must equal (dataDT(0).mkString(","))
      outRows(1).mkString(",") must equal (dataDT(1).mkString(","))
    }
  }

  import com.rojoma.json.v3.interpolation._

  val schemaJson = Seq("jobj" -> SoQLObject,
    "jarray" -> SoQLArray,
    "json" -> SoQLJson)

  val dataJson: Seq[Array[SoQLValue]] = Seq(
    Array(SoQLObject(j"""{"apple": 5, "bananas": [456, "Chiquitos"]}"""),
      SoQLArray (j"""[1, true, null, "not really"]"""),
      SoQLJson  (j"""56.7890"""))
  )

  test("Can serialize SoQL rows with Json data types") {
    writeThenRead { os =>
      val w = new SoQLPackWriter(schemaJson)
      w.write(os, dataJson.toIterator)
    } { dis =>
      val r = new SoQLPackIterator(dis)
      r.geomIndex must equal (-1)
      r.schema must equal (schemaJson)
      val outRows = r.toList
      outRows must have length (dataJson.length)
      outRows(0) must equal (dataJson(0))
    }
  }
}
