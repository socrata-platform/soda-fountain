CREATE TABLE IF NOT EXISTS datasets (
  resource_name                    VARCHAR(64) NOT NULL,
  dataset_system_id                VARCHAR(64) NOT NULL,
  created_at                       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  PRIMARY KEY (resource_name),
  UNIQUE (resource_name, dataset_system_id)
);