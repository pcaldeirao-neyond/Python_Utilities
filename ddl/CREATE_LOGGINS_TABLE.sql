CREATE SCHEMA IF NOT EXISTS operations_logging;

DROP TABLE IF EXISTS operations_logging.process_logging;

CREATE TABLE IF NOT EXISTS operations_logging.process_logging (
  id_tab           BIGINT COMMENT 'Auto-increment primary key',
  run_id           STRING COMMENT 'UUID correlating all log entries of a single execution run',
  process          STRING COMMENT 'Main process or pipeline name',
  subprocess       STRING COMMENT 'Sub-process or module name',
  step             STRING COMMENT 'Granular step within the sub-process',
  status           STRING COMMENT 'STARTED | RUNNING | COMPLETED | FAILED | SKIPPED',
  log_level        STRING COMMENT 'DEBUG | INFO | WARNING | ERROR | CRITICAL',
  log_msg          STRING COMMENT 'Human-readable log message',
  error_msg        STRING COMMENT 'Error description when applicable',
  error_traceback  STRING COMMENT 'Full Python traceback when applicable',
  exec_query       STRING COMMENT 'SQL or command that was executed',
  rows_affected    BIGINT COMMENT 'Number of rows processed in this step',
  target_table     STRING COMMENT 'Target table being operated on',
  duration_seconds DOUBLE COMMENT 'Elapsed seconds for this step or process',
  started_at       TIMESTAMP COMMENT 'When the step or process started',
  ended_at         TIMESTAMP COMMENT 'When the step or process ended',
  ref_date         STRING COMMENT 'Business reference date',
  exec_user        STRING COMMENT 'Databricks user who executed the process',
  exec_script    STRING COMMENT 'Script path that triggered the log entry',
  created_at       TIMESTAMP COMMENT 'When this log row was written'
)
COMMENT 'Centralized process execution logging table v2.0';
