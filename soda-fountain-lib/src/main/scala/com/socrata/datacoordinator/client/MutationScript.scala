package com.socrata.datacoordinator.client

import java.io._
import com.rojoma.json.ast._
import com.rojoma.json.util.JsonUtil

class MutationScript(
        user: String,
        copyInstruction: DatasetCopyInstruction,
        instructions: Iterator[DataCoordinatorInstruction]){
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
        case i: ColumnMutation  => {
          if (rowDataDeclared){
            out.write("null,")
            rowDataDeclared = false
          }
          out.write(i.toString)
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

  private def topLevelCommandBase(schema: Option[String]) = {
    val base = Map( "c" -> JString(copyInstruction.command), "user" -> JString(user))
    val baseAndSchema = schema match {
      case Some(s) => base + ("schema" -> JString(s))
      case None => base
    }
    baseAndSchema
  }

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
