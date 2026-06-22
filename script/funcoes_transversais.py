#!/usr/bin/env python
import sys
import math
import time
import psycopg2
import re
import subprocess
import os
import argparse
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql import SparkSession, Row, HiveContext
from pyspark import SparkContext, SparkConf
import logging


CONF = SparkConf().setAppName("captools").set("hive.exec.dynamic.partition", "true").set("hive.exec.dynamic.partition.mode", "nonstrict").set("hive.exec.parallel", "true").set("hive.merge.mapfiles", "true").set("hive.merge.mapredfiles", "true").set(
    "sqlContext.sql.crossJoin.enabled", "true").set("hive.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
sc = SparkContext.getOrCreate(conf=CONF)

sqlContext = HiveContext(sc)
sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

sqlContext.setConf("hive.exec.parallel", "true")

sqlContext.setConf("hive.merge.mapfiles", "true")

sqlContext.setConf("hive.merge.mapredfiles", "true")

sqlContext.setConf("hive.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

sqlContext.setConf("sqlContext.sql.crossJoin.enabled", "true")


#-------------------------------------------------
#FUNÇOES SQL
#-------------------------------------------------

def __executeSql_log(query):
    try:
        procID = main.procID
        sqlContext.sql(query)
        LOGGER.info(" Executed: \n {} \n".format(query))
    except Exception as e:
        LOGGER.error("Error executing: \n {} \n exception {}".format(query, e))
        # sys.exit(1)
        raise
    return

def create_table(target_table, schema):
    query = "CREATE TABLE IF NOT EXISTS {} {}".format(target_table, schema)
    __executeSql_log(query)
    return

def drop_table(target_table):
    query = "DROP TABLE IF EXISTS {}".format(target_table)
    __executeSql_log(query)
    return

def drop_auxiliar_tables(target_table_list):
    for target_table in target_table_list:
        drop_table(target_table)
    return

def drop_create_table(target_table, schema):
    drop_table(target_table)
    create_table(target_table, schema)
    return

def drop_partition_table(target_table, partition):
    query = "ALTER TABLE {} DROP IF EXISTS PARTITION({})".format(target_table, partition)
    __executeSql_log(query)
    return

def with_insert_overwrite_table(with_query, target_table, select_query):
    query = " {} INSERT OVERWRITE TABLE {} {}".format(with_query, target_table, select_query)
    __executeSql_log(query)
    return

def insert_overwrite_table(target_table, select_query):
    with_insert_overwrite_table("", target_table, select_query)
    return

def with_insert_overwrite_partition_query(with_query, target_table, partition, select_query):
    query = "{} INSERT OVERWRITE TABLE {} PARTITION({}) {}".format(with_query, target_table, partition, select_query)
    __executeSql_log(query)
    return

def insert_overwrite_partition_table(target_table, partition, select_query):
    with_insert_overwrite_partition_query("", target_table, partition, select_query)
    return

def insert_query(target_table, select_query):
    query = "INSERT INTO {} {}".format(target_table, select_query)
    __executeSql_log(query)
    return

def create_replace_view(target_view, select_query):
    query = " CREATE OR REPLACE VIEW {} AS {}".format(target_view, select_query)
    __executeSql_log(query)
    return

def create_replace_view_with(with_query, target_view, select_query):
    query = " CREATE OR REPLACE VIEW {} AS {} {}".format(target_view, with_query, select_query)
    __executeSql_log(query)
    return

def truncate_table(target_table):
    query = "TRUNCATE TABLE {}".format(target_table)
    __executeSql_log(query)
    return

def create_bck_table(target_table, ref_date=""):
    if ref_date != "":
        where = "WHERE ref_date ='{}'".format(ref_date)
    else:
        where = ""
    query = "SELECT * FROM {} {}".format(target_table, where)
    bck_name = target_table + "_bck"
    drop_create_table(bck_name, query)

def createDataframe(list, schema):
    try:
        df = sqlContext.createDataFrame(list, schema)
        LOGGER.info("Creating dataframe = {} ".format(df))
    except Exception as e:
        print('\nError creating dataframe: {}'.format(e))
        raise
    return df

def invalidateTableMetadata(table, flg_update=False, partition="default"):
    LOGGER.info("\n IN: invalidadeTableMetadata \n")
    query = "INVALIDATE METADATA {}".format(table)
    try:
        impalaHost = propertiesFileParser.get("IMPALA_CONNECTION", "HOST").strip()
        impalaPort = propertiesFileParser.get("IMPALA_CONNECTION", "PORT").strip()
        impalaConStr = impalaHost + ":" + impalaPort
        kerberosUser = propertiesFileParser.get("KERBEROS", "USER").strip()
        # kerberosKeytabFilename = propertiesFileParser.get("KERBEROS", "KEYTAB_FILENAME").strip()
        LOGGER.info("kerberosUser = {}".format(kerberosUser))

        os.environ['PYTHON_EGG_CACHE'] = '/tmp'
        if flg_update:
            if partition == 'default':
                aux_partition = "ref_date = \"{}\", cod_visao={}".format(refDate, codVisao)
            else:
                aux_partition = partition
            retcode = subprocess.call("kinit {} -k -t *.keytab* && impala-shell -k -i {} -q '{}' ".format(kerberosUser, impalaConStr, query), shell=True)
            if retcode == 0:
                # Executa o compute_statistics por table e partição
                import generico.script.main_statistics as stats
                stats.compute_statistics(table, aux_partition, True)
                LOGGER.info("\n Executed invalidate metadata + update stats for table {}\n".format(table))
            else:
                LOGGER.error("\n Failed to execute invalidate metadata + update stats for table {}, return code: {}\n".format(table, retcode))
        else:
            retcode = subprocess.call("kinit {} -k -t *.keytab* && impala-shell -k -i '{}' -q '{};'".format(kerberosUser, impalaConStr, query), shell=True)
            if retcode == 0:
                LOGGER.info("\n Executed invalidate metadata for table {}\n".format(table))
            else:
                LOGGER.error("\n Failed to execute invalidate metadata for table {}, return code: {}\n".format(table, retcode))
        LOGGER.info("\n OFF: invalidadeTableMetadata \n")
    except Exception as e:
        LOGGER.error("\n Error invalidating table metadata or update stats for table {}\n".format(str(e)))
        raise
    return

def insert_table_hist_dynamic_partition(initial_table, target_table, partition_date, partition_fields):
    LOGGER.info(" \n INICIO DO PROCESSO: {} \n".format(target_table))
    columns = get_col_names(initial_table)
    columns_partition = str(columns[:2])
    for field in partition_fields:
        LOGGER.info(field)
        # LOGGER.info('INIT' + str(columns))
        columns_partition = columns_partition + ', ' + field
        # columns = str(columns[1:])
        columns.remove(field)
        # LOGGER.info('FINAL' + str(columns))

    columns_partition = columns_partition.replace("'", "")
    columns_partition = columns_partition.replace('[', '')
    columns_partition = columns_partition.replace(']', '')

    columns = str(columns[2:])
    columns = columns.replace("'", "")
    columns = columns.replace('[', '')
    columns = columns.replace(']', '')
    for field in partition_fields:
        columns = columns.replace(", " + str(field), "")

    if partition_date is "":
        query = " SELECT {}, {} FROM {}".format(columns, columns_partition, initial_table)
    else:
        query = " SELECT {}, {} FROM {} WHERE ref_date = '{}'".format(columns, columns_partition, initial_table, partition_date)

    part = "{}".format(columns_partition)
    insert_overwrite_partition_table(target_table, part, query)
    main.invalidateTableMetadata(target_table)

    LOGGER.info(" \n FIM DO PROCESSO: {} \n".format(target_table))

    return

def select_collect(select_query):
    try:
        res = sqlContext.sql("{}".format(select_query)).collect()
        LOGGER.info("Collected: \n {}".format(select_query))
    except Exception as e:
        res = None
        LOGGER.error("Error in select_collect: \n query {} \n exception {} \n".format(select_query, e))
        raise
    return res

def get_col_names(table):
    columnsaux = sqlContext.table(table)
    columns = columnsaux.schema.names
    columns = [aux.lower() for aux in columns]
    return columns

#-------------------------------------------------
#FUNÇOES de DATA
#-------------------------------------------------
def ref_date_max_month(target_table, date_column, REFDATE_AAAAMM):
    select_ref_date_max = select_collect(" SELECT NVL(max({1}),'') as date_column FROM {0} WHERE {1} like '{2}%' ".format(target_table, date_column, REFDATE_AAAAMM))
    ref_date_max = select_ref_date_max[0].date_column
    return ref_date_max

def get_max_date(data_date_part, table, refdate):
    max_data_date_part = select_collect("select max({}) as max_date_part from {} where substr({},1,7) <= '{}' ".format(data_date_part,
    table, data_date_part, ref_date[:-3]))
    return max_data_date_part

# returns whether or not the partition exists in the target hive table
def partition_exists(target_table, partition):
    partitionList = sqlContext.sql("SHOW PARTITIONS {}".format(target_table)).collect()
    for partitionRow in partitionList:
        part = partitionRow[0]
        # LOGGER.info("{} = {}".format(part, partition))
        if (str(part) == partition):
            return True
    return False

def last_day_of_month(sdate):
    from datetime import timedelta
    next_month = sdate.replace(day=28) + timedelta(days=4)
    return next_month - timedelta(days=next_month.day)

def get_col_names_with_alias(table, alias):
    columnsaux = sqlContext.table(table)
    columns = columnsaux.schema.names
    columns = [alias + "." + aux.lower() for aux in columns]
    return columns

def sumToRefDate(texto, num, leap):
    # returns:
    # M - last day of the shifted month
    # Y - last day and month of the shifted year
    from datetime import datetime, timedelta
    from dateutil.relativedelta import relativedelta

    try:
        type = ""
        date_leap = ""
        date = texto  # "2019-12-31"
        year = int(date[0:4])
        month = int(date[5:7])
        day = int(date[-2:])

        currentDate = datetime(year, month, day)

        if (leap == "M"):
            date_leap = last_day_of_month(currentDate + relativedelta(months=num))
            type = " Month:"
        elif (leap == "Y"):
            date_leap = last_day_of_month(datetime(year + num, 12, 31))
            type = " Year:"
        else:
            raise Exception("'leap' should be either Y or M: {}".format(leap))

        print('ref_date: {}'.format(date))
        print('Last{} {}'.format(type, date_leap.strftime('%Y-%m-%d')))

    except Exception as e:
        LOGGER.error("\nError in getLastRefDate: {} Illegal leap type {}".format(texto, leap))
        raise

    return date_leap.strftime('%Y-%m-%d')

def getStartsExpression(condition_col, field):
    try:
        expression = condition_col

        # caso considerar todos os registos
        if (expression == ''):
            return expression
        # caso considerar registos especificos
        else:
            # tratamento do campo
            expression = re.sub(r"((not)\s+)?(in)\s+[\(]\s*[\\'']", "", expression)
            expression = re.sub(r"\s+", "", expression.strip())
            expression = expression.replace("'", "")
            expression = expression.replace(")", "")
            list_values = expression.split(',')

            first_iteration = True

            # percorre todos os valores da lista para obter query
            for v in list_values:
                aux_v = "'" + v + "%'"

                if aux_v == "'%'": aux_v = "''"

                if first_iteration:
                    expression = field + " LIKE " + aux_v
                    first_iteration = False
                else:
                    expression += " OR " + field + " LIKE " + aux_v

            # condicoes inclusao/exclusao
            not_in_condition = "not in ("
            if not_in_condition in condition_col:
                expression = " !( " + expression + " )"
            else:
                expression = " ( " + expression + " )"

    except Exception as e:
        LOGGER.error('\n Error getStartsExpression exception: {} \n'.format(str(e)))
        LOGGER.error("\n Error in getStartsExpression: expression {}".format(expression))
        sys.exit(-1)

    return expression

def getContainsExpression(condition_col, field):
    try:
        expression = condition_col
        # caso considerar todos os registos
        if (expression == ''):
            return expression
        else:
            list_ands = expression.upper().split('AND')
            first_iteration_ands = True

            # para permitir parametrizar varios idcombs na mesma coluna, separados por AND
            for a in list_ands:
                expression = a
                expression = re.sub(r"((NOT)\s+)?(IN)\s+[\(]\s*[\\'']", "", expression)
                expression = re.sub(r"\s+", " ", expression.strip())
                expression = expression.replace("'", "")
                expression = expression.replace(")", "")
                expression = expression.replace('*', '%')

                list_values = expression.split(',')
                first_iteration_values = True

                # percorre todos os valores da lista para obter query
                for v in list_values:
                    # remove espacos no inicio e fim
                    v = v.strip()
                    if first_iteration_values:
                        expression = "UPPER(" + field + ") LIKE " + "'%" + v + "%' "
                        first_iteration_values = False
                    else:
                        expression += "OR UPPER(" + field + ") LIKE " + "'%" + v + "%' "

                # condicoes de inclusao/exclusao
                not_in_condition = "NOT IN ("
                if not_in_condition in condition_col:
                    expression = " !( " + expression + " )"
                else:
                    expression = " ( " + expression + " )"

                if first_iteration_ands:
                    expression_final = expression
                    first_iteration_ands = False
                else:
                    expression_final = expression_final + " AND " + expression

    except Exception as e:
        LOGGER.error('\n Error getContainsExpression exception: {} \n'.format(str(e)))
        LOGGER.error("\nError in getContainsExpression: expression {}".format(expression))
        sys.exit(-1)

    return expression_final


def get_param_value(param_table, refdate, ambito=""):
    # Obter os campos necessarios - Adaptar consoante a necessidade
    fields = "UPPER(campo1) AS campo 1, UPPER(campo2) AS campo2, UPPER(campo3) AS campo3, UPPER(campo4) AS campo4"

    # obter ambito se existir (parametro opcional)
    add_ambito = ""
    if ambito != "":
        add_ambito = "AND ambito='{}'".format(ambito)

    # Obter valor da parametrização
    params = select_collect("SELECT {} FROM {} WHERE ref_date = '{}' {}".format(fields, param_table, refdate, add_ambito))

    if params != []:
        # Variavel para guardar as condicoes tratadas
        dic_final_values = defaultdict(list)
        # Obter o nome dos campos exceto campos tecnicos
        field_names = get_col_names(param_table)[:-4]
        # Obter o nome do campo a calcular no ambito da parametrizacao
        param_field_name = field_names[-1]
        for param in params:
            list_condicoes = []
            # Retirar espacos em branco, colocar em maiusculas e NVL caso o valor seja NULL
            campo1 = "regexp_replace(UPPER(NVL(" + param.campo1.encode() + ",'')), ' ', '')"
            campo2 = "regexp_replace(UPPER(NVL(" + param.campo2.encode() + ",'')), ' ', '')"
            campo3 = "regexp_replace(UPPER(NVL(" + param.campo3.encode() + ",'')), ' ', '')"
            # Campo direto que nao necessita tratamentos
            campo4 = getContainsExpression(param.campo4.encode(), "campo4") if param.campo4 else ""
            # Valor objetivo da parametrizacao
            valor_param = param.valor.encode()

            # Transformar regras em  estilo de condicoes like (campo1 like '%')
            condicao1 = getContainsExpression(param.campo1.encode(), campo1)
            condicao2 = getContainsExpression(param.campo2.encode(), campo2)
            condicao3 = getContainsExpression(param.campo3.encode(), campo3)

            if condicao1 != "":
                list_condicoes.append(condicao1)

            if condicao2 != "":
                list_condicoes.append(condicao2)

            if condicao3 != "":
                list_condicoes.append(condicao3)

            if condicao4 != "":
                list_condicoes.append(campo4)

            # Construcao das condicoes WHEN do CASE
            # Transforma lista iterando AND entre os elementos da lista
            # Ex: list =[a,b,c]-> a AND b AND c
            condicao_when = " AND ".join(list_condicoes)

            condicao_when = "WHEN " + condicao_when + " THEN '" + valor_param + "'"

            dic_final_values[param_field_name].append(condicao_when)

        # Construcao final do CASE
        case_final = "CASE \n"
        for k in dic_final_values.keys():
            for v in dic_final_values[k]:
                case_final = case_final + v + " \n"

            case_final = case_final + "ELSE '' END AS " + k

        return case_final


#-------------------------------------------------
#FUNÇOES LOGGING FRAMEWORK
#-------------------------------------------------

#Obtem a ultima LOG_MSG dado um processo da tabela de logs
def get_last_log(processo, target_table):
    query = """SELECT LOG_MSG FROM {} WHERE PROCESSO = '{}' ORDER BY id DESC LIMIT 1""".format(target_table, processo)
    result = sqlContext.sql(query)
    if result.count() > 0:
        return result.first()["LOG_MSG"]
    return ""

def insert_log(
    process,
    subprocess,
    status,
    log_msg,
    exec_query,
    run_id,
    step,
    log_level,
    error_msg,
    error_traceback,
    rows_affected,
    target_table,
    duration_seconds,
    started_at,
    ended_at,
    ref_date,
    exec_user,
    exec_script,
    created_at,
    log_table):

    id_tab = select_collect("SELECT MAX(id_tab) FROM {}".format(log_table))
    if id_tab == []:
        id_tab = 0
    else:
        id_tab = id_tab[0][0] + 1


    query = f"""
    INSERT INTO {log_table} (
       id_tab, run_id, process, subprocess, step, status, log_level, log_msg, error_msg, error_traceback,
        exec_query, rows_affected, target_table, duration_seconds, started_at, ended_at,
        ref_date, exec_user, exec_script, created_at
    ) VALUES ( '{id_tab}',
        '{run_id}', '{process}', '{subprocess}', '{step}', '{status}', '{log_level}', '{log_msg}', '{error_msg}', '{error_traceback}',
        '{exec_query}', {rows_affected if rows_affected is not None else 'NULL'}, '{target_table}', {duration_seconds if duration_seconds is not None else 'NULL'},
        '{started_at}', '{ended_at}', '{ref_date}', '{exec_user}', '{exec_script}', '{created_at}'
    )
    """
    sqlContext.sql(query)

def insert_start_log(process, subprocess, step, log_level, exec_query, target_table, log_table):
    insert_log(process, subprocess, step, "STARTED", log_level, "Início do processo de carga", '','',exec_query, '001', 0, target_table, 0, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), datetime.now().strftime("%Y-%m-%d %H:%M:%S"), '2026-05-31', 'USER','SCRIPT',datetime.now().strftime("%Y-%m-%d %H:%M:%S"),log_table)
    return

def insert_ended_log(process, subprocess, step, log_level, exec_query, target_table, log_table):
    insert_log(process, subprocess, step, "ENDED", log_level, "Fim do processo de carga", '','',exec_query,'001', 0, target_table, 0, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), datetime.now().strftime("%Y-%m-%d %H:%M:%S"), '2026-05-31', 'USER','SCRIPT',datetime.now().strftime("%Y-%m-%d %H:%M:%S"),log_table)
    return

def insert_error_log(process, subprocess, step, log_level, exec_query, target_table, log_table):
    insert_log(process, subprocess, step, "ERROR", log_level, "Erro do processo de carga", '','',exec_query,'001', 0, target_table, 0, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), datetime.now().strftime("%Y-%m-%d %H:%M:%S"), '2026-05-31', 'USER','SCRIPT',datetime.now().strftime("%Y-%m-%d %H:%M:%S"),log_table)
    return

#Obtem o log mais recente por processo na tabela de logs
def get_last_status(table, processo=''): 
    query = f"""
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY processo ORDER BY id DESC
            ) AS _rn
            FROM {table}
            WHERE PROCESSO ='{processo}'
        )
        WHERE _rn = 1
        ORDER BY processo
    """
    return sqlContext.sql(query).drop("_rn")


# Obtem os logs mais recentes da execução de um processo
# Identifica primeiro o run_id mais recente e depois faz o join com a
# tabela original para apresentar o histórico de execução por ordem cronológica
def get_last_run(table, processo=''):
    query = f"""
        WITH latest AS (
            SELECT run_id
            FROM {table}
            WHERE processo = '{processo}'
            ORDER BY id DESC
            LIMIT 1
        )
        SELECT l.* FROM {table} l
        INNER JOIN latest lr ON l.run_id = lr.run_id
        ORDER BY l.id
    """
    return sqlContext.sql(query)


# Obtem todos os processos com o estado de INICIADO mas não
# têm estado de TERMINADO ou ERRO
def get_current_runs(table):
    """
    Pattern: NOT EXISTS anti-join — efficient even on large tables
    because it short-circuits on the first matching end-state row.
    """
    query = f"""
        SELECT
            s.processo,
            s.subprocesso,
            s.run_id,
            s.exec_user,
            s.exec_notebook,
            s.created_at AS started_at,
            ROUND(
                (UNIX_TIMESTAMP(CURRENT_TIMESTAMP())
                    - UNIX_TIMESTAMP(s.created_at)) / 60, 1
            ) AS minutes_running
        FROM {table} s
        WHERE s.status = 'STARTED'
            AND NOT EXISTS (
                SELECT 1 FROM {table} e
                WHERE e.run_id = s.run_id
                AND e.status IN ('COMPLETED', 'FAILED')
            )
        ORDER BY s.created_at DESC
    """
    return sqlContext.sql(query)

# Obtem os processos que estão a correr há mais de um limite de horas
# Limite de horas pre-definido: 4
def get_stale_processes(table, threshold_hours=4):
    t = table
    query = f"""
        SELECT
            s.processo,
            s.subprocesso,
            s.run_id,
            s.exec_user,
            s.exec_notebook,
            s.created_at AS started_at,
            ROUND(
                (UNIX_TIMESTAMP(CURRENT_TIMESTAMP())
                    - UNIX_TIMESTAMP(s.created_at)) / 3600, 2
            ) AS hours_running
        FROM {table} s
        WHERE s.status = 'STARTED'
            AND NOT EXISTS (
                SELECT 1 FROM {table} e
                    WHERE e.run_id = s.run_id
                    AND e.status IN ('COMPLETED', 'FAILED')
            )
            AND (UNIX_TIMESTAMP(CURRENT_TIMESTAMP())
                - UNIX_TIMESTAMP(s.created_at)) / 3600 > {threshold_hours}
        ORDER BY hours_running DESC
    """
    return sqlContext.sql(query)


#-------------------------------------------------
#FUNÇOES ESTATISTICA DE PROCESSOS
#-------------------------------------------------

# Apresenta estatistica sumariada de um dado processo
# Cada linha representa uma execução
def get_process_summary(table, processo='', last_n_days=30):
    t = table
    query = f"""
        SELECT
            processo,
            run_id,
            MIN(created_at)  AS run_start,
            MAX(created_at)  AS run_end,
            -- Process-level completion: only count step='' entries
            MAX(CASE WHEN status = 'COMPLETED'
                        AND (step = '' OR step IS NULL)
                        THEN 1 ELSE 0 END)  AS completed,
            MAX(CASE WHEN status = 'FAILED'
                        AND (step = '' OR step IS NULL)
                        THEN 1 ELSE 0 END)  AS had_errors,
            COUNT(*)                      AS log_entries,
            MAX(duration_seconds)         AS duration_s,
            MAX(rows_affected)            AS total_rows,
            FIRST(exec_user)              AS exec_user
        FROM {table}
        WHERE created_at >= CURRENT_TIMESTAMP() - INTERVAL {last_n_days} DAYS
            and PROCESSO = '{processo}'
        GROUP BY processo, run_id
        ORDER BY run_start DESC
    """
    return sqlContext.sql(query)


# Obtem métricas como a taxa de sucesso/erro de um processo dado um número de dias
# Caso o processo falhe, isso será contado como um erro independentemente de ter
# subprocessos a correr com sucesso
# Número de dias pre-definido: 30
def get_reliability_report(table, last_n_days=30):
    query = f"""WITH runs AS (
        SELECT
            processo,
            run_id,
            -- Only process-level entries determine run outcome
            MAX(CASE WHEN status = 'COMPLETED'
                      AND (step = '' OR step IS NULL)
                     THEN 1 ELSE 0 END) AS ok,
            MAX(CASE WHEN status = 'FAILED'
                      AND (step = '' OR step IS NULL)
                     THEN 1 ELSE 0 END) AS ko,
            MAX(duration_seconds) AS dur
        FROM {table}
        WHERE created_at >= CURRENT_TIMESTAMP() - INTERVAL {last_n_days} DAYS
        GROUP BY processo, run_id
    )
    SELECT
        processo,
        COUNT(*)                                     AS total_runs,
        SUM(ok)                                      AS successful,
        SUM(ko)                                      AS failed,
        ROUND(100.0 * SUM(ok) / COUNT(*), 1)         AS success_rate_pct,
        ROUND(AVG(dur), 2)                           AS avg_duration_s,
        ROUND(MAX(dur), 2)                           AS max_duration_s
    FROM runs
    GROUP BY processo
    ORDER BY success_rate_pct ASC
    """
    return sqlContext.sql(query)

# Obtem todos os processos com um que terminaram com ERRO nos ultimos dias
# Janela de dias pre-definida: 7  
def get_recent_errors(table, last_n_days=7, processo=''):
    query = f"""
        SELECT
            processo, subprocesso, step, run_id,
            log_msg, error_msg, error_traceback,
            exec_user, exec_notebook, created_at
        FROM {table}
        WHERE log_level IN ('ERROR', 'CRITICAL')
          AND created_at >= CURRENT_TIMESTAMP() - INTERVAL {last_n_days} DAYS
          AND PROCESSO ='{processo}'
        ORDER BY created_at DESC
    """
    return sqlContext.sql(query)

# Obtem por ordem cronológica todos os passos executados de uma execução 
def get_run(run_id, table):
    query = f"""
        SELECT * FROM {table}
        WHERE run_id = '{ProcessLogger._esc(run_id)}'
        ORDER BY id
    """
    return sqlContext.sql(query)


#inicialização do logger
logging.basicConfig(format="%(asctime)s:%(levelname)s:%(filename)s:%(message)s")
LOGGER = logging.getLogger("funcoes_transversais")