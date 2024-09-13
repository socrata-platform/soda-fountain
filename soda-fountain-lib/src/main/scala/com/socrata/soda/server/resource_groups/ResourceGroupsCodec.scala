package com.socrata.soda.server.resource_groups

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.codec.JsonDecode.DecodeResult
import com.rojoma.json.v3.codec.{JsonDecode, JsonEncode}
import com.rojoma.json.v3.matcher.{PObject, Variable}
import com.rojoma.json.v3.util.{NoTag, SimpleHierarchyCodecBuilder}

import java.util.UUID
import scala.collection.JavaConverters._

import com.socrata.resource_groups.models.api.{ResourceGroupsObject, Registration, RegistrationDetails, Secondaries}

object ResourceGroupsCodec {

  implicit object registrationCodec extends JsonEncode[Registration] with JsonDecode[Registration] {
    val resourceGroupIdVar = Variable[UUID]
    val isMovingVar = Variable[Boolean]
    val registrationPattern = PObject(
      "resourceGroupId" -> resourceGroupIdVar,
      "isMoving" -> isMovingVar
    )

    override def encode(x: Registration): JValue = registrationPattern.generate(
      resourceGroupIdVar := x.getResourceGroupId,
      isMovingVar := x.getIsMoving
    )

    override def decode(x: JValue): DecodeResult[Registration] = registrationPattern.matches(x).right.map { r =>
      new Registration(
        resourceGroupIdVar(r),
        isMovingVar(r)
      )
    }
  }

  implicit object registrationDetailsCodec extends JsonEncode[RegistrationDetails] with JsonDecode[RegistrationDetails] {
    val nameVar = Variable[String]
    val registrationDetailsPattern = PObject(
      "name" -> nameVar,
    )

    override def encode(x: RegistrationDetails): JValue = registrationDetailsPattern.generate(
      nameVar := x.getName,
    )

    override def decode(x: JValue): DecodeResult[RegistrationDetails] = registrationDetailsPattern.matches(x).right.map { r =>
      val y = new RegistrationDetails()
      y.setName(nameVar(r))
      y
    }
  }

  implicit object secondariesCodec extends JsonEncode[Secondaries] with JsonDecode[Secondaries] {
    val secondariesVar = Variable[Set[String]]
    val secondariesPattern = PObject(
      "secondaries" -> secondariesVar,
    )

    override def encode(x: Secondaries): JValue = secondariesPattern.generate(
      secondariesVar := x.getSecondaries.asScala.toSet,
    )

    override def decode(x: JValue): DecodeResult[Secondaries] = secondariesPattern.matches(x).right.map { r =>
      new Secondaries(secondariesVar(r).asJava)
    }
  }

  implicit val resourceGroupsCodec: JsonEncode[ResourceGroupsObject] with JsonDecode[ResourceGroupsObject] = SimpleHierarchyCodecBuilder[ResourceGroupsObject](NoTag)
    .branch[Registration]
    .branch[RegistrationDetails]
    .branch[Secondaries]
    .build

}
