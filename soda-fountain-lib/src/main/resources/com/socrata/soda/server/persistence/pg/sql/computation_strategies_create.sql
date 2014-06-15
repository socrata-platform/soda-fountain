CREATE TABLE IF NOT EXISTS computation_strategies (
  dataset_system_id                     VARCHAR(64) NOT NULL,
  column_id                             VARCHAR(64) NOT NULL,
  computation_strategy_type             VARCHAR(64) NOT NULL,
  recompute                             BOOLEAN NOT NULL,
  source_columns                        TEXT[],
  parameters                            TEXT,
  UNIQUE (dataset_system_id, column_id),
  FOREIGN KEY (dataset_system_id, column_id) REFERENCES columns (dataset_system_id, column_id)
);