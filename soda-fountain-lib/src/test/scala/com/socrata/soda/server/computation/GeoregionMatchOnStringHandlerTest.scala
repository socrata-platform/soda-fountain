package com.socrata.soda.server.computation

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.conversions._
import com.socrata.soda.server.computation.ComputationHandler.MaltypedDataEx
import com.socrata.soda.server.id.ColumnId
import com.socrata.soda.server.persistence.{MinimalColumnRecord, ComputationStrategyRecord, ColumnRecordLike}
import com.socrata.soda.server.wiremodels.ComputationStrategyType
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types._
import com.typesafe.config.ConfigFactory
import org.apache.curator.x.discovery._
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.{Matchers, PrivateMethodTester, FunSuiteLike}
import org.scalatest.mock.MockitoSugar
import scala.collection.JavaConverters._

trait FakeDiscovery extends MockitoSugar {
  val discovery = mock[ServiceDiscovery[Any]]
  val builder = mock[ServiceProviderBuilder[Any]]
  val provider = mock[ServiceProvider[Any]]

  when(discovery.serviceProviderBuilder()).thenReturn(builder)
  when(builder.providerStrategy(any[ProviderStrategy[Any]])).thenReturn(builder)
  when(builder.serviceName(anyString)).thenReturn(builder)
  when(builder.build()).thenReturn(provider)
}

class GeoregionMatchOnStringHandlerTest extends FunSuiteLike with FakeDiscovery with PrivateMethodTester with Matchers {
  val testConfig = ConfigFactory.parseMap(Map(
    "service-name"    -> "region-coder",
    "batch-size"      -> 2,
    "max-retries"     -> 1,
    "retry-wait"      -> "500ms",
    "connect-timeout" -> "5s",
    "read-timeout"    -> "5s"
  ).asJava)

  val handler = new GeoregionMatchOnStringHandler(testConfig, discovery)

  val genEndpoint         = PrivateMethod[String]('genEndpoint)
  val extractSourceColumn = PrivateMethod[Option[String]]('extractSourceColumnValueFromRow)
  val toJValue            = PrivateMethod[JValue]('toJValue)

  def sourceColumn = MinimalColumnRecord(ColumnId("abcd-2345"), ColumnName("my_source_column"), SoQLNull, false, None)

  test("genEndpoint - valid column definition") {
    val columnDef = mock[ColumnRecordLike]
    when(columnDef.computationStrategy).thenReturn(Some(
      ComputationStrategyRecord(
        ComputationStrategyType.GeoRegionMatchOnString,
        true,
        Some(Seq(sourceColumn)),
        Some(JObject(Map("region" -> JString("sfo_zipcodes"), "column" -> JString("zip")))))))

    val endpoint = handler.invokePrivate(genEndpoint(columnDef))
    endpoint should be ("/regions/sfo_zipcodes/stringcode?column=zip")
  }

  test("genEndpoint - parameters are missing") {
    val columnDef = mock[ColumnRecordLike]
    when(columnDef.computationStrategy).thenReturn(Some(
      ComputationStrategyRecord(
        ComputationStrategyType.GeoRegionMatchOnString,
        true,
        Some(Seq(sourceColumn)),
        None)))

    val ex = the [IllegalArgumentException] thrownBy {
      handler.invokePrivate(genEndpoint(columnDef)).size
    }
    ex.getMessage should include ("Computation strategy parameters were invalid")
  }

  test("genEndpoint - region is missing") {
    val columnDef = mock[ColumnRecordLike]
    when(columnDef.computationStrategy).thenReturn(Some(
      ComputationStrategyRecord(
        ComputationStrategyType.GeoRegionMatchOnString,
        true,
        Some(Seq(sourceColumn)),
        Some(JObject(Map("column" -> JString("zip")))))))

    val ex = the [IllegalArgumentException] thrownBy {
      handler.invokePrivate(genEndpoint(columnDef)).size
    }
    ex.getMessage should include ("parameters does not contain 'region'")
  }

  test("genEndpoint - column is missing") {
    val columnDef = mock[ColumnRecordLike]
    when(columnDef.computationStrategy).thenReturn(Some(
      ComputationStrategyRecord(
        ComputationStrategyType.GeoRegionMatchOnString,
        true,
        Some(Seq(sourceColumn)),
        Some(JObject(Map("region" -> JString("sfo_zipcodes")))))))

    val ex = the [IllegalArgumentException] thrownBy {
      handler.invokePrivate(genEndpoint(columnDef)).size
    }
    ex.getMessage should include ("parameters does not contain 'column'")
  }

  test("extractSourceColumnValueFromRow - source column contains a string") {
    val value = handler.invokePrivate(extractSourceColumn(
      Map("name" -> SoQLText("foo")), ColumnName("name")))
    value should be (Some("foo"))
  }

  test("extractSourceColumnValueFromRow - source column contains a number") {
    val value = handler.invokePrivate(extractSourceColumn(
      Map("name" -> SoQLNumber(new java.math.BigDecimal(500))), ColumnName("name")))
    value should be (Some("500"))
  }

  test("extractSourceColumnValueFromRow - source column is missing") {
    val value = handler.invokePrivate(extractSourceColumn(
      Map("other_field" -> SoQLText("giraffe")), ColumnName("name")))
    value should be (None)
  }

  test("extractSourceColumnValueFromRow - source column is SoQLNull") {
    val value = handler.invokePrivate(extractSourceColumn(
      Map("name" -> SoQLNull), ColumnName("name")))
    value should be (None)
  }

  test("extractSourceColumnValueFromRow - source column is an unexpected type") {
    a [MaltypedDataEx] should be thrownBy {
      handler.invokePrivate(extractSourceColumn(
        Map("name" -> SoQLJson(JNumber(5))), ColumnName("name")))
    }
  }
}
