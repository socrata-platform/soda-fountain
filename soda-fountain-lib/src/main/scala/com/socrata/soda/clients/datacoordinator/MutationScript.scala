package com.socrata.soda.clients.datacoordinator

import java.io._

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.io._
import com.rojoma.json.v3.util.JsonUtil
import org.slf4j.LoggerFactory

class MutationScript(
        user: String,
        copyInstruction: DatasetCopyInstruction,
        instructions: Iterator[DataCoordinatorInstruction],
        reportingInterval: Int = 50000){
  val log = LoggerFactory.getLogger(getClass)

  def it : Iterator[JsonEvent] = {
    var rowDataDeclared = false
    var declaredOptions = RowUpdateOption.default
    var seqNum = 0

    val dcInstructions = instructions.map { instruction =>
      seqNum += 1
      if (seqNum % reportingInterval == 0)
        log.info(s"MutationScript: instruction # $seqNum [$instruction]")

      val ops = new scala.collection.mutable.ListBuffer[JValue]
      instruction match {
        case _:ColumnMutation | _:RollupMutation | _:IndexMutation  => {
          if (rowDataDeclared){
            rowDataDeclared = false
            ops += JNull
          }
          ops += instruction.asJson
        }
        case i: RowUpdate => {
          i match {
            case options: RowUpdateOption => {
              if (options != declaredOptions){
                if (rowDataDeclared){
                  rowDataDeclared = false
                  ops += JNull
                }
              }
              declaredOptions = options
              if (!rowDataDeclared){
                rowDataDeclared = true
                ops += declaredOptions.asJson
              }
            }
            case _ => {
              if (!rowDataDeclared) {
                rowDataDeclared = true
                ops += declaredOptions.asJson
              }
              ops += i.asJson
            }
          }
        }
      }
      ops
    }

    val streamableInstructions : Iterator[JsonEvent] =
      Iterator.single(StartOfArrayEvent()(Position.Invalid)) ++
      JValueEventIterator(JObject(topLevelCommand)) ++
      dcInstructions.flatMap{ lbuf => lbuf.flatMap{ jval => JValueEventIterator(jval)}} ++
      Iterator.single(EndOfArrayEvent()(Position.Invalid))

    streamableInstructions
  }

  def streamJson(os:OutputStream){
    val out = new OutputStreamWriter(os)
    streamJson(out)
  }

  def streamJson(out: Writer){
    var rowDataDeclared = false
    var declaredOptions = RowUpdateOption.default
    var seqNum = 0

    out.write('[')
    out.write(JsonUtil.renderJson(JObject(topLevelCommand)))

    while (instructions.hasNext) {
      val instruction = instructions.next
      seqNum += 1
      if (seqNum % reportingInterval == 0)
        log.info(s"MutationScript streamJson: instruction # $seqNum [$instruction]")

      out.write(',')
      instruction match {
        case _:ColumnMutation | _:RollupMutation | _:IndexMutation => {
          if (rowDataDeclared){
            out.write("null,")
            rowDataDeclared = false
          }
          out.write(instruction.toString)
        }
        case i: RowUpdate => {
          i match {
            case ops: RowUpdateOption => {
              if (ops != declaredOptions){
                if (rowDataDeclared){
                  out.write("null,")
                  rowDataDeclared = false
                }
              }
              declaredOptions = ops
              if (!rowDataDeclared){
                out.write(declaredOptions.toString)
                rowDataDeclared = true
              }
            }
            case _ => {
              if (!rowDataDeclared) {
                out.write(declaredOptions.toString)
                out.write(",")
                rowDataDeclared = true
              }
              out.write(i.toString)
            }
          }
        }
      }
    }
    out.write(']')
    out.flush()
  }

  private def topLevelCommandBase(schema: String, expectedDataVersion: Option[Long]) = {
    val base = Map[String, JValue]("c" -> JString(copyInstruction.command), "user" -> JString(user), "schema" -> JString(schema))
    expectedDataVersion.fold(base) { edv => base + ("data_version" -> JNumber(edv)) }
  }

  def topLevelCommand: Map[String, JValue] = {
    copyInstruction match {
      case i: CreateDataset => Map("resource" -> JString(i.resource.name), "locale" -> JString(i.locale), "c" -> JString(copyInstruction.command), "user" -> JString(user))
      case i: UpdateDataset => topLevelCommandBase(i.schema, i.expectedDataVersion)
      case i: CopyDataset   => topLevelCommandBase(i.schema, i.expectedDataVersion) + ("copy_data" -> JBoolean(i.copyData))
      case i: PublishDataset=> topLevelCommandBase(i.schema, i.expectedDataVersion)
      case i: DropDataset => topLevelCommandBase(i.schema, i.expectedDataVersion)
    }
  }

}
