package com.socrata.datacoordinator.client

import com.rojoma.json.ast.JString

class MutationScript( dataset: String,
                      user: String,
                      fatalRowErrors: Boolean,
                      copyInstruction: DatasetCopyInstruction,
                      columnMutations: Option[List[ColumnMutationInstruction]],
                      rowUpdates: Option[Iterable[RowUpdateInstruction]]) {

}
