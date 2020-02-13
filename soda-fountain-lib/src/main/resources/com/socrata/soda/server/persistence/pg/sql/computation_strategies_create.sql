CREATE TABLE IF NOT EXISTS computation_strategies (
  dataset_system_id                     VARCHAR(64) NOT NULL,
  column_id                             VARCHAR(64) NOT NULL,
  computation_strategy_type             VARCHAR(64) NOT NULL,
  recompute                             BOOLEAN NOT NULL,
  source_columns                        TEXT[],
  parameters                            TEXT,
  UNIQUE (dataset_system_id, column_id)
);

-- Explicitly name the foreign key for backwards compatibility as postgres 12.0 changed the default name for foreign keys to include all columns
ALTER TABLE computation_strategies ADD CONSTRAINT computation_strategies_dataset_system_id_fkey FOREIGN KEY (dataset_system_id, column_id) REFERENCES columns(dataset_system_id, column_id);
