package com.socrata.soda.server.computation

import java.math

import com.rojoma.json.v3.ast._
import com.socrata.geocoders.caching.MapCacheClient
import com.socrata.geocoders.{LatLon, Address, CachingGeocoderAdapter, NoopGeocoder}
import com.socrata.http.server.util.RequestId
import com.socrata.soda.server.computation.ComputationHandler.MaltypedDataEx
import com.socrata.soda.server.highlevel.RowDataTranslator
import com.socrata.soda.server.highlevel.RowDataTranslator.{UpsertAsSoQL, DeleteAsCJson}
import com.socrata.soda.server.id.ColumnId
import com.socrata.soda.server.metrics.Metrics.Metric
import com.socrata.soda.server.persistence.{MinimalColumnRecord, ComputationStrategyRecord, ColumnRecordLike}
import com.socrata.soda.server.wiremodels.ComputationStrategyType
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types._
import com.typesafe.config.ConfigFactory
import com.vividsolutions.jts.geom.{GeometryFactory, Coordinate}
import org.scalatest.{Matchers, FunSuiteLike}
import scala.collection.JavaConverters._

class GeocodingHandlerTest extends FunSuiteLike with Matchers {

  val addr1 = Address(Some("83 S King St #107"), Some("Seattle")   , Some("WA"), Some("98104"), None)
  val addr2 = Address(None                     , Some("Victoria")  , Some("BC"), None         , "CA")
  val addr3 = Address(Some("101 Alice St")     , Some("Wonderland"), None      , None         , None)
  val addr4 = Address(None                     , Some("Seattle")   , None      , None         , None)

  val ll1 = Some(LatLon(47.6303, -122.3148))
  val ll2 = Some(LatLon(48.4475, -123.3336))
  val ll3 = None
  val ll4 = Some(LatLon(47.6303, -122.3148))

  val cacheData = Seq((addr1, ll1), (addr2, ll2), (addr4, ll4))

  val point1 = """{"type":"Point","coordinates":[47.6303,-122.3148]}"""
  val point2 = """{"type":"Point","coordinates":[48.4475,-123.3336]}"""
  val point4 = """{"type":"Point","coordinates":[47.6303,-122.3148]}"""

  def toSoQL(opt: Option[String]) = opt match {
    case Some(str) => SoQLText(str)
    case None => SoQLNull
  }

  def toRow(addr: Address, date: String) = {
    UpsertAsSoQL(Map(
      "addr-esss" -> toSoQL(addr.address),
      "city-yyyy" -> toSoQL(addr.city),
      "stat-eeee" -> toSoQL(addr.state),
      "zipp-pppp" -> toSoQL(addr.zip),
      "coun-tryy" -> toSoQL(Some(addr.country)),
      "date-eeee" -> SoQLText(date)
    ))
  }

  val row1 = toRow(addr1, "12/31/2013")
  val row2 = toRow(addr2, "11/30/2013")
  val row3 = toRow(addr3, "12/4/2013")
  val row4 = toRow(addr4, "1/14/2014")


  val testRows = Seq[RowDataTranslator.Computable](
    DeleteAsCJson(JString("abcd-1234")),
    row1,
    row2,
    DeleteAsCJson(JString("efgh-5678")),
    row3,
    row4,
    DeleteAsCJson(JString("ijkl-9012"))
  )

  val geometryFactory = new GeometryFactory()

  def toRowResult(row: UpsertAsSoQL, ll: Option[LatLon]) = ll match {
    case Some(LatLon(lat, lon)) =>
      UpsertAsSoQL(row.rowData + ("poin-tttt" -> SoQLPoint(geometryFactory.createPoint(new Coordinate(lat, lon)))))
    case None => row
  }

  val testRowsResult = Seq[RowDataTranslator.Computable](
    DeleteAsCJson(JString("abcd-1234")),
    toRowResult(row1, ll1),
    toRowResult(row2, ll2),
    DeleteAsCJson(JString("efgh-5678")),
    toRowResult(row3, ll3),
    toRowResult(row4, ll4),
    DeleteAsCJson(JString("ijkl-9012"))
  )

  def withMocks[A](f: (MapCacheClient, GeocodingHandler) => A) = {
    val cache = new MapCacheClient
    val geocoder = new CachingGeocoderAdapter(cache, NoopGeocoder, { _: Long =>})
    val config = ConfigFactory.parseMap(Map(
      "batch-size" -> 2
    ).asJava)
    val handler = new GeocodingHandler(config, geocoder, { _: Metric => })
    f(cache, handler)
  }

  // source columns
  def addressSC = MinimalColumnRecord(ColumnId("addr-esss"), ColumnName("my_address_column"), SoQLNull, false, None)
  def citySC    = MinimalColumnRecord(ColumnId("city-yyyy"), ColumnName("my_city_column"), SoQLNull, false, None)
  def stateSC   = MinimalColumnRecord(ColumnId("stat-eeee"), ColumnName("my_state_column"), SoQLNull, false, None)
  def zipSC     = MinimalColumnRecord(ColumnId("zipp-pppp"), ColumnName("my_zip_column"), SoQLNull, false, None)
  def countrySC = MinimalColumnRecord(ColumnId("coun-tryy"), ColumnName("my_country_column"), SoQLNull, false, None)
  def sourceColumns = Some(Seq(addressSC, citySC, stateSC, zipSC, countrySC))

  def params = Some(JObject(Map(
    "address" -> JString("my_address_column"),
    "city" -> JString("my_city_column"),
    "state" -> JString("my_state_column"),
    "zip" -> JString("my_zip_column"),
    "country" -> JString("my_country_column")
  )))

  def columnRecordLike(colId: ColumnId, colName: ColumnName, strategy: Option[ComputationStrategyRecord]) = {
    new ColumnRecordLike {
      override val id: ColumnId = colId
      override val fieldName: ColumnName = colName
      override val typ: SoQLType = SoQLPoint
      override val computationStrategy: Option[ComputationStrategyRecord] = strategy
      override val isInconsistencyResolutionGenerated: Boolean = false
    }
  }

  def computationStrategyRecord(sources: Option[Seq[MinimalColumnRecord]], params: Option[JObject]) = {
    ComputationStrategyRecord(
      ComputationStrategyType.GeoCoding,
      recompute = true,
      sources,
      params
    )
  }

  def computedColumn(id: String, name: String, sources: Option[Seq[MinimalColumnRecord]], params: Option[JObject]) = {
    columnRecordLike(
      ColumnId(id),
      ColumnName(name),
      Some(computationStrategyRecord(sources, params))
    )
  }

  test("Should accept a full valid column definition") {
    withMocks { (_, instance) =>
      instance.compute(
        RequestId.generate(),
        Iterator[RowDataTranslator.Computable](),
        computedColumn("abcd-7777", "my_point_column", sourceColumns, params)
      )
    }
  }

  test("Should accept a partial but valid column definition") {
    withMocks { (_, instance) =>
      instance.compute(
        RequestId.generate(),
        Iterator[RowDataTranslator.Computable](),
        computedColumn(
          "abcd-7777",
          "my_point_column",
          Some(Seq(citySC)),
          Some(JObject(Map("city" -> JString("my_city_column"))))
        )
      )
    }
  }

  test("Should accept a valid column definition missing some source columns") {
    withMocks { (_, instance) =>
      instance.compute(
        RequestId.generate(),
        Iterator[RowDataTranslator.Computable](),
        computedColumn("abcd-7777", "my_point_column", Some(Seq(citySC)), params)
      )
    }
  }

  test("Should accept a valid column definition missing all source columns") {
    withMocks { (_, instance) =>
      instance.compute(
        RequestId.generate(),
        Iterator[RowDataTranslator.Computable](),
        computedColumn("abcd-7777", "my_point_column", Some(Seq()), params)
      )
    }
  }

  test("Should accept a valid column definition with empty source columns or params") {
    withMocks { (_, instance) =>
      instance.compute(
        RequestId.generate(),
        Iterator[RowDataTranslator.Computable](),
        computedColumn("abcd-7777", "my_point_column", Some(Seq()), Some(JObject(Map())))
      )
    }
  }

  test("Should not accept a column definition with no source columns or params") {
    withMocks { (_, instance) =>
      the [GeocodingHandler.InvalidComputationStrategyParamsException] thrownBy {
        instance.compute(
          RequestId.generate(),
          Iterator[RowDataTranslator.Computable](),
          computedColumn("abcd-7777", "my_point_column", Some(Seq()), None)
        )
      }
    }

    withMocks { (_, instance) =>
      the [GeocodingHandler.InvalidComputationStrategyException] thrownBy {
        instance.compute(
          RequestId.generate(),
          Iterator[RowDataTranslator.Computable](),
          computedColumn("abcd-7777", "my_point_column", None, Some(JObject(Map())))
        )
      }
    }
  }

  test("Should not accept a column definition with missing computation strategy") {
    withMocks { (_, instance) =>
      the [IllegalArgumentException] thrownBy {
        instance.compute(
          RequestId.generate(),
          Iterator[RowDataTranslator.Computable](),
          columnRecordLike(
            ColumnId("abcd-7777"),
            ColumnName("my_point_column"),
            None
          )
        )
      }
    }
  }

  test("Should be able to geocode addresses and splice points into rows") {
    withMocks { (cache, instance) =>
      cache.cache(cacheData)
      val computed = instance.compute(
        RequestId.generate(),
        testRows.toIterator,
        computedColumn("poin-tttt", "my_point_column", sourceColumns, params)
      )
      computed.toSeq should be (testRowsResult)
    }
  }

  test("Should be able to geocode addresses where the zip is a number... :/") {
    withMocks { (cache, instance) =>
      cache.cache(cacheData)
      val row = UpsertAsSoQL(Map(
        "addr-esss" -> toSoQL(addr1.address),
        "city-yyyy" -> toSoQL(addr1.city),
        "stat-eeee" -> toSoQL(addr1.state),
        "zipp-pppp" -> SoQLNumber(new java.math.BigDecimal(98104)),
        "coun-tryy" -> toSoQL(Some(addr1.country)),
        "date-eeee" -> SoQLText("12/31/2013")
      ))
      val computed = instance.compute(
        RequestId.generate(),
        Seq(row).toIterator,
        computedColumn("poin-tttt", "my_point_column", sourceColumns, params)
      )
      computed.toSeq should be (Seq(toRowResult(row, ll1)))
    }
  }

  test("Should fail if other fields in the address are a number") {
    withMocks { (cache, instance) =>
      cache.cache(cacheData)
      val row = UpsertAsSoQL(Map(
        "addr-esss" -> SoQLNumber(new math.BigDecimal(83)),
        "city-yyyy" -> toSoQL(addr1.city),
        "stat-eeee" -> toSoQL(addr1.state),
        "zipp-pppp" -> toSoQL(addr1.zip),
        "coun-tryy" -> toSoQL(Some(addr1.country)),
        "date-eeee" -> SoQLText("12/31/2013")
      ))
      the [MaltypedDataEx] thrownBy {
        instance.compute(
          RequestId.generate(),
          Seq(row).toIterator,
          computedColumn("poin-tttt", "my_point_column", sourceColumns, params)
        ).toSeq // since it is lazy
      }
    }
  }

  test("Should fail to geocode when a value is of a bad type") {
    withMocks { (cache, instance) =>
      cache.cache(cacheData)
      val row = UpsertAsSoQL(Map(
        "addr-esss" -> toSoQL(addr1.address),
        "city-yyyy" -> toSoQL(addr1.city),
        "stat-eeee" -> toSoQL(addr1.state),
        "zipp-pppp" -> SoQLDouble(98104),
        "coun-tryy" -> toSoQL(Some(addr1.country)),
        "date-eeee" -> SoQLText("12/31/2013")
      ))
      the [MaltypedDataEx] thrownBy {
        instance.compute(
          RequestId.generate(),
          Seq(row).toIterator,
          computedColumn("poin-tttt", "my_point_column", sourceColumns, params)
        ).toSeq // since it is lazy
      }
    }
  }

}
