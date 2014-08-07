UPDATE computation_strategies cs
SET source_columns =
  (SELECT ARRAY(
                 SELECT column_id
                  FROM columns c
                  WHERE c.dataset_system_id = cs.dataset_system_id
                    AND column_name = ANY (cs.source_columns)
                )
  )
WHERE cs.source_columns <@ ARRAY(
                                 SELECT text(column_name)
                                  FROM columns c
                                  WHERE c.dataset_system_id = cs.dataset_system_id);