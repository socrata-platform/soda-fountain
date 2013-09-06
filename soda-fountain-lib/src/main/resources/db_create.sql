CREATE TABLE IF NOT EXISTS datasets (
  resource_name_casefolded         VARCHAR(64) NOT NULL,
  resource_name                    VARCHAR(64) NOT NULL,
  dataset_system_id                VARCHAR(64) NOT NULL,
  created_at                       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  name                             TEXT NOT NULL,
  description                      TEXT NOT NULL,
  PRIMARY KEY (resource_name_casefolded),
  UNIQUE (dataset_system_id)
);

CREATE TABLE IF NOT EXISTS columns (
  dataset_system_id                VARCHAR(64) NOT NULL REFERENCES datasets(dataset_system_id),
  column_name_casefolded           VARCHAR(64) NOT NULL,
  column_name                      VARCHAR(64) NOT NULL,
  column_id                        CHAR(9) NOT NULL,
  created_at                       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  name                             TEXT NOT NULL,
  description                      TEXT NOT NULL,
  UNIQUE (dataset_system_id, column_id),
  UNIQUE (dataset_system_id, column_name_casefolded)
);
