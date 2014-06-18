package com.socrata.soda.clients.datacoordinator

import java.io._
import com.rojoma.json.ast._
import com.rojoma.json.util.JsonUtil
import com.rojoma.json.io._

class MutationScript(
        user: String,
        copyInstruction: DatasetCopyInstruction,
        instructions: Iterator[DataCoordinatorInstruction]){

  def it : Iterator[JsonEvent] = {
    var rowDataDeclared = false
    var declaredOptions = RowUpdateOptionChange()

    val dcInstructions = instructions.map{ instruction =>
      val ops = new scala.collection.mutable.ListBuffer[JValue]
      instruction match {
        case _:ColumnMutation | _:RollupMutation  => {
          if (rowDataDeclared){
            rowDataDeclared = false
            ops += JNull
          }
          ops += instruction.asJson
        }
        case i: RowUpdate => {
          i match {
            case options: RowUpdateOptionChange => {
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
      Iterator.single(StartOfArrayEvent()) ++
      com.rojoma.json.io.JValueEventIterator(JObject(topLevelCommand)) ++
      dcInstructions.flatMap{ lbuf => lbuf.flatMap{ jval => JValueEventIterator(jval)}} ++
      Iterator.single(EndOfArrayEvent())

    streamableInstructions
  }

  def streamJson(os:OutputStream){
    val out = new OutputStreamWriter(os)
    streamJson(out)
  }

  def streamJson(out: Writer){
    var rowDataDeclared = false
    var declaredOptions = RowUpdateOptionChange()

    out.write('[')
    out.write(JsonUtil.renderJson(JObject(topLevelCommand)))

    while (instructions.hasNext){
      val instruction = instructions.next
      out.write(',')
      instruction match {
        case _:ColumnMutation | _:RollupMutation  => {
          if (rowDataDeclared){
            out.write("null,")
            rowDataDeclared = false
          }
          out.write(instruction.toString)
        }
        case i: RowUpdate => {
          i match {
            case ops: RowUpdateOptionChange => {
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

  private def topLevelCommandBase(schema: String) =
    Map( "c" -> JString(copyInstruction.command), "user" -> JString(user), "schema" -> JString(schema))

  def topLevelCommand: Map[String, JValue] = {
    copyInstruction match {
      case i: CreateDataset => Map("locale" -> JString(i.locale), "c" -> JString(copyInstruction.command), "user" -> JString(user))
      case i: UpdateDataset => topLevelCommandBase(i.schema)
      case i: CopyDataset   => topLevelCommandBase(i.schema) + ("copy_data" -> JBoolean(i.copyData))
      case i: PublishDataset=> i.snapshotLimit match {
        case Some(s)  => topLevelCommandBase(i.schema) + ("snapshot_limit" -> JNumber(s))
        case None     => topLevelCommandBase(i.schema)
      }
      case i: DropDataset => topLevelCommandBase(i.schema)
    }
  }

}
