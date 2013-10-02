CREATE TABLE IF NOT EXISTS datasets (
  resource_name_casefolded         VARCHAR(128) NOT NULL,
  resource_name                    VARCHAR(128) NOT NULL,
  dataset_system_id                VARCHAR(64) NOT NULL,
  created_at                       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  name                             TEXT NOT NULL,
  description                      TEXT NOT NULL,
  locale                           VARCHAR(64) NOT NULL,
  schema_hash                      VARCHAR(64) NOT NULL,
  primary_key_column_id            VARCHAR(64) NOT NULL,
  PRIMARY KEY (resource_name_casefolded),
  UNIQUE (dataset_system_id)
);

CREATE TABLE IF NOT EXISTS columns (
  dataset_system_id                     VARCHAR(64) NOT NULL REFERENCES datasets(dataset_system_id),
  column_name_casefolded                VARCHAR(128) NOT NULL,
  column_name                           VARCHAR(128) NOT NULL,
  column_id                             VARCHAR(64) NOT NULL,
  type_name                             VARCHAR(64) NOT NULL,
  created_at                            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  name                                  TEXT NOT NULL,
  description                           TEXT NOT NULL,
  is_inconsistency_resolution_generated BOOLEAN NOT NULL,
  UNIQUE (dataset_system_id, column_id),
  UNIQUE (dataset_system_id, column_name_casefolded)
);
