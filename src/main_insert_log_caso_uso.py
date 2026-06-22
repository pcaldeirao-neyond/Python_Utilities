import funcoes_transversais as func

import time

func.insert_start_log(
    process="1",
    subprocess="carga_dados",
    step="",
    log_level="INFO",
    exec_query="SELECT * FROM tabela_origem",
    target_table="tab_exemplo",
    log_table="operations_logging.process_logging"
)

time.sleep(5)


func.insert_ended_log(
    process="1",
    subprocess="carga_dados",
    step="",
    log_level="INFO",
    exec_query="SELECT * FROM tabela_origem",
    target_table="tab_exemplo",
    log_table="operations_logging.process_logging"
)

time.sleep(5)

func.insert_error_log(
    process="1",
    subprocess="carga_dados",
    step="",
    log_level="ERROR",
    exec_query="SELECT * FROM tabela_origem",
    target_table="tab_exemplo",
    log_table="operations_logging.process_logging"
)