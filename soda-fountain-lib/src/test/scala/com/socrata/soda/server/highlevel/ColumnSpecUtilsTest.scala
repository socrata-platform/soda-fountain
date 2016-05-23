package com.socrata.soda.server.highlevel

import com.rojoma.json.v3.ast.JObject
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.interpolation._
import com.socrata.computation_strategies._
import com.socrata.soda.server.highlevel.ColumnSpecUtils._
import com.socrata.soda.server.id.ColumnId
import com.socrata.soda.server.wiremodels._
import com.socrata.soql.types.{SoQLPoint, SoQLNumber, SoQLText, SoQLType}
import org.scalatest.{FunSuite, Matchers}
import java.security.SecureRandom
import com.socrata.soql.environment.ColumnName

class ColumnSpecUtilsTest extends FunSuite with Matchers {
  lazy val rng = new scala.util.Random(new SecureRandom())
  lazy val columnSpecUtils = new ColumnSpecUtils(rng)

  def test(columnName: String, uCompStrategy: Option[UserProvidedComputationStrategySpec], expectValid: Boolean) =
    columnSpecUtils.validColumnName(ColumnName(columnName), uCompStrategy) should be (expectValid)

  test("validColumnName - non computed column, valid name") {
    test("address", None, true)
  }

  test("validColumnName - non computed column, :@ name") {
    test(":@address", None, true)
  }

  test("validColumnName - non computed column, blank name") {
    test("", None, false)
  }

  test("validColumnName - non computed column, invalid chars") {
    test("add,ress", None, false)
  }

  test("validColumnName - non computed column, underscore") {
    test("add_ress", None, true)
  }

  test("validColumnName - non computed column, hyphen") {
    test("add-ress", None, true)
  }

  test("validColumnName - non computed column, colon") {
    test(":address", None, false)
  }

  test("validColumnName - computed column, colon") {
    test(":location", Some(UserProvidedComputationStrategySpec(Some(StrategyType.Test), None, None)), true)
  }

  test("validColumnName - computed column, no colon allowed") {
    test("location", Some(UserProvidedComputationStrategySpec(Some(StrategyType.Geocoding), None, None)), true)
  }

  test("validColumnName - computed column, no colon not allowed") {
    test("region", Some(UserProvidedComputationStrategySpec(Some(StrategyType.GeoRegionMatchOnPoint), None, None)), false)
  }

  test("validColumnName - computed column, collides with system column") {
    test(":created_at",  Some(UserProvidedComputationStrategySpec(Some(StrategyType.Test), None, None)), false)
  }

  def testFreeze[T <: ColumnSpecUtils.CreateResult](existingColumns: Map[ColumnName, (ColumnId, SoQLType)],
                                                    ucs: UserProvidedColumnSpec, expected: T) =
    columnSpecUtils.freezeForCreation(existingColumns, ucs) match {
      case Success(resultSpec) =>
        expected match {
          case Success(expectedSpec) =>
            resultSpec.fieldName should be (expectedSpec.fieldName)
            resultSpec.datatype should be (expectedSpec.datatype)
            resultSpec.computationStrategy should be (expectedSpec.computationStrategy)
          case other =>
            resultSpec should be (expected) // this should always fail (but will give a good message)
        }
      case other =>
        other should be (expected)
    }

  test("freezeForCreation - test computed column") {
    val existingColumns = Map[ColumnName,(ColumnId, SoQLType)](
      ColumnName("my_column") -> ((ColumnId("1111-1111"), SoQLText))
    )
    val ucs = UserProvidedColumnSpec(
      id = None,
      fieldName = Some(ColumnName(":@test_computed")),
      datatype = Some(SoQLText),
      delete = None,
      computationStrategy = Some(UserProvidedComputationStrategySpec(
        strategyType = Some(StrategyType.Test),
        sourceColumns = Some(Seq("my_column")),
        parameters = Some(json"{concat_text:'foo'}".asInstanceOf[JObject])
      )))
    testFreeze(existingColumns, ucs, Success(
      ColumnSpec(
        ColumnId("???"),
        ColumnName(":@test_computed"),
        SoQLText,
        Some(ComputationStrategySpec(
          StrategyType.Test,
          Some(Seq(SourceColumnSpec(ColumnId("1111-1111"), ColumnName("my_column")))),
          Some(json"{concat_text:'foo'}".asInstanceOf[JObject])
        )))))
  }

  test("freezeForCreation - test computed column UnknownComputationStrategySourceColumn") {
    val existingColumns = Map[ColumnName, (ColumnId, SoQLType)]()
    val ucs = UserProvidedColumnSpec(
      id = None,
      fieldName = Some(ColumnName(":@test_computed")),
      datatype = Some(SoQLText),
      delete = None,
      computationStrategy = Some(UserProvidedComputationStrategySpec(
        strategyType = Some(StrategyType.Test),
        sourceColumns = Some(Seq("my_column")),
        parameters = Some(json"{concat_text:'foo'}".asInstanceOf[JObject])
      )))
    testFreeze(existingColumns, ucs, UnknownComputationStrategySourceColumn)
  }

  test("freezeForCreation - test computed column ComputationStrategyNoStrategyType") {
    val existingColumns = Map[ColumnName, (ColumnId, SoQLType)](
      ColumnName("my_column") -> ((ColumnId("1111-1111"), SoQLText))
    )
    val ucs = UserProvidedColumnSpec(
      id = None,
      fieldName = Some(ColumnName(":@test_computed")),
      datatype = Some(SoQLText),
      delete = None,
      computationStrategy = Some(UserProvidedComputationStrategySpec(
        strategyType = None,
        sourceColumns = Some(Seq("my_column")),
        parameters = Some(json"{concat_text:'foo'}".asInstanceOf[JObject])
      )))
    testFreeze(existingColumns, ucs, ComputationStrategyNoStrategyType)
  }

  test("freezeForCreation - test computed column WrongDatatypeForComputationStrategy") {
    val existingColumns = Map[ColumnName, (ColumnId, SoQLType)](
      ColumnName("my_column") -> ((ColumnId("1111-1111"), SoQLText))
    )
    val ucs = UserProvidedColumnSpec(
      id = None,
      fieldName = Some(ColumnName(":@test_computed")),
      datatype = Some(SoQLNumber),
      delete = None,
      computationStrategy = Some(UserProvidedComputationStrategySpec(
        strategyType = Some(StrategyType.Test),
        sourceColumns = Some(Seq("my_column")),
        parameters = Some(json"{concat_text:'foo'}".asInstanceOf[JObject])
      )))
    testFreeze(existingColumns, ucs, WrongDatatypeForComputationStrategy(found = SoQLNumber, required = SoQLText))
  }

  test("freezeForCreation - test computed column InvalidComputationStrategy") {
    val existingColumns = Map[ColumnName, (ColumnId, SoQLType)](
      ColumnName("my_column") -> ((ColumnId("1111-1111"), SoQLText))
    )
    val ucs = UserProvidedColumnSpec(
      id = None,
      fieldName = Some(ColumnName(":@test_computed")),
      datatype = Some(SoQLText),
      delete = None,
      computationStrategy = Some(UserProvidedComputationStrategySpec(
        strategyType = Some(StrategyType.Test),
        sourceColumns = Some(Seq("my_column")),
        parameters = None
      )))
    testFreeze(existingColumns, ucs, InvalidComputationStrategy(MissingParameters(StrategyType.Test)))
  }

  test("freezeForCreation - test computed column strategy type \"geocoding\"") {
    val existingColumns = Map[ColumnName, (ColumnId, SoQLType)](
      ColumnName("street_address") -> ((ColumnId("2222-2222"), SoQLText)),
      ColumnName("zip_code") -> ((ColumnId("3333-3333"), SoQLText))
    )
    val ucs = UserProvidedColumnSpec(
      id = None,
      fieldName = Some(ColumnName("location")),
      datatype = Some(SoQLPoint),
      delete = None,
      computationStrategy = Some(UserProvidedComputationStrategySpec(
        strategyType = Some(StrategyType.Geocoding),
        sourceColumns = Some(Seq("street_address", "zip_code")),
        parameters = Some(JsonEncode.toJValue(GeocodingParameterSchema(
          sources = GeocodingSources(
            address = Some("street_address"),
            locality = None,
            region = None,
            subregion = None,
            postalCode = Some("zip_code"),
            country = None
          ),
          defaults = GeocodingDefaults(
            address = None,
            locality = Some("Seattle"),
            region = Some("WA"),
            subregion = None,
            postalCode = None,
            country = "US"
          ),
          version = "v1"
        )).asInstanceOf[JObject])
      )))
    testFreeze(existingColumns, ucs, Success(
      ColumnSpec(
        ColumnId("???"),
        ColumnName("location"),
        SoQLPoint,
        Some(ComputationStrategySpec(
          StrategyType.Geocoding,
          Some(Seq(
            SourceColumnSpec(ColumnId("2222-2222"), ColumnName("street_address")),
            SourceColumnSpec(ColumnId("3333-3333"), ColumnName("zip_code"))
          )),
          Some(JsonEncode.toJValue(GeocodingParameterSchema(
            sources = GeocodingSources(
              address = Some("2222-2222"),
              locality = None,
              region = None,
              subregion = None,
              postalCode = Some("3333-3333"),
              country = None
            ),
            defaults = GeocodingDefaults(
              address = None,
              locality = Some("Seattle"),
              region = Some("WA"),
              subregion = None,
              postalCode = None,
              country = "US"
            ),
            version = "v1"
          )).asInstanceOf[JObject])
        )))))
  }
}
