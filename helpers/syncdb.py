import subprocess
import os
import yaml
import pdb
import mysql.connector
from pymysql.converters import escape_string
import traceback
import re
import datetime
import decimal
from . import logging


source_cnx = None
target_cnx = None
source_cursor = None
target_cursor = None


def syncdbs():
    dbs = {}

    logging.logger.info("Reading the database yaml file")
    with open(os.getenv("DBS_FILE"), "r") as f:
        dbs = yaml.safe_load(f)

    for db in dbs.get("databases"):
        logging.logger.info("=" * 50)
        try:
            logging.logger.info(f"Starting the sync for {db.get('name')}")
            source = db.get("source")
            destination = db.get("destination")
            sync_databases(source, destination)
            logging.logger.info("Database sync completed successfully")
        except Exception as e:
            logging.logger.error(f"Error while syncing {db.get('name')}")
            logging.logger.error(e)
            traceback.print_exc()


def create_db_connections(source_con_str, target_con_str):
    global source_cnx, target_cnx, source_cursor, target_cursor
    source_cnx = mysql.connector.connect(**source_con_str)
    target_cnx = mysql.connector.connect(**target_con_str)
    source_cursor = source_cnx.cursor()
    target_cursor = target_cnx.cursor()


def sync_databases(source_con_str, target_con_str):
    global source_cnx, target_cnx, source_cursor, target_cursor
    create_db_connections(source_con_str, target_con_str)
    source_tables = get_tables()

    # Synchronize the data for each table
    for table in source_tables:
        table_name = table[0]
        sync_tables(table_name)
        sync_data(table_name)
        logging.logger.info(f"Synced data for table {table_name}")

    source_functions = get_functions()
    for function in source_functions:
        function_name = function[1]
        sync_functions(function_name)

    source_procedures = get_stored_procedures()
    for procedure in source_procedures:
        procedure_name = procedure[1]
        sync_stored_procedures(procedure_name)

    # Close the cursors and connections
    source_cursor.close()
    target_cursor.close()
    source_cnx.close()
    target_cnx.close()


def sync_tables(table_name):
    # check if the table is present in the target database
    target_cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    if not target_cursor.fetchone():
        # if not, get the table structure from the source database
        source_cursor.execute(f"SHOW CREATE TABLE {table_name}")
        create_table_query = source_cursor.fetchone()[1]
        # and create the table in the target database
        target_cursor.execute(create_table_query)
        logging.logger.info(f"Created table {table_name}")
        target_cnx.commit()
    else:
        sync_columns(table_name)


def sync_stored_procedures(procedure_name):
    # check if the procedure is present in the target database
    target_cursor.execute(
        f"SHOW PROCEDURE STATUS WHERE Db = '{target_cnx._database}' AND Name = '{procedure_name}'"
    )
    if target_cursor.fetchone():
        target_cursor.execute(f"DROP PROCEDURE {procedure_name}")

    source_cursor.execute(f"SHOW CREATE PROCEDURE {procedure_name}")
    create_procedure_query = source_cursor.fetchone()[2]

    # remove definer from the procedure using regex
    create_procedure_query = re.sub(
        r"DEFINER=`[^`]*`@`[^`]*`",
        "",
        create_procedure_query,
        count=1,
        flags=re.IGNORECASE,
    )

    # and create the procedure in the target database
    target_cursor.execute(create_procedure_query)
    logging.logger.info(f"Created procedure {procedure_name}")
    target_cnx.commit()


def sync_functions(function_name):
    # check if the function is present in the target database
    target_cursor.execute(
        f"SHOW FUNCTION STATUS WHERE Db = '{target_cnx._database}' AND Name = '{function_name}'"
    )
    if target_cursor.fetchone():
        target_cursor.execute(f"DROP FUNCTION {function_name}")

    source_cursor.execute(f"SHOW CREATE FUNCTION {function_name}")
    create_function_query = source_cursor.fetchone()[2]
    # and create the function in the target database
    target_cursor.execute(create_function_query)
    logging.logger.info(f"Created function {function_name}")
    target_cnx.commit()


def sync_columns(table_name):
    # if the table already exist in the target database, check the columns
    target_cursor.execute(f"DESCRIBE {table_name}")
    target_columns = set(col[0] for col in target_cursor.fetchall())
    source_cursor.execute(f"DESCRIBE {table_name}")
    source_columns = set(col[0] for col in source_cursor.fetchall())
    # if the columns in the source database are not present in the target database
    for col in source_columns - target_columns:
        # then add them to the target database
        source_cursor.execute(f"SHOW COLUMNS FROM {table_name} LIKE '{col}'")
        column_def = source_cursor.fetchone()
        target_cursor.execute(
            f"ALTER TABLE {table_name} ADD COLUMN {column_def[0]} {column_def[1]}"
        )
        logging.logger.info(f"Added column {col} to {table_name}")
    target_cnx.commit()


def get_columns(table_name):
    source_cursor.execute(f"SHOW COLUMNS FROM {table_name}")
    source_columns = source_cursor.fetchall()

    columns_def = {}

    index = 0
    for col in source_columns:
        columns_def[col[0]] = {
            "index": index,
            "type": col[1].split("(")[0].upper(),
            "auto_increment": col[5] == "auto_increment",
        }
        index += 1
    return columns_def


def get_identity_column(columns_def):
    identity_column = None
    for col in columns_def:
        if columns_def[col]["auto_increment"]:
            identity_column = col
    return identity_column


def sync_data(table_name):

    columns_def = get_columns(table_name)
    identity_column = get_identity_column(columns_def)

    source_cursor.execute(f"SELECT * FROM {table_name}")
    source_data = source_cursor.fetchall()
    # check if the data is already present in the target database
    if identity_column is None:

        logging.logger.info(f"Deleting all data from {table_name}")

        target_cursor.execute(f"DELETE FROM {table_name}")
        for row in source_data:
            values = format_row_data(row, columns_def)
            # Execute the insert statement
            query = f"INSERT INTO `{table_name}` VALUES ({', '.join(values)})"
            logging.logger.info(f"Executing query: {query}")
            target_cursor.execute(query)
            logging.logger.info(f"Inserted data into {table_name}")

    else:
        logging.logger.info("Primary key found, checking for existing data")
        for row in source_data:
            primary_key_index = columns_def[identity_column]["index"]

            target_cursor.execute(
                f"SELECT * FROM `{table_name}` WHERE `{identity_column}` = {row[primary_key_index]}"
            )

            values = format_row_data(row, columns_def)
            # Execute the insert statement
            if target_cursor.fetchone():
                logging.logger.info(
                    f"Updating data for {table_name} with id {row[primary_key_index]}"
                )
                update_query = f"UPDATE `{table_name}` SET {', '.join([f'`{col}` = {val}' for col, val in zip(source_cursor.column_names, values)])} WHERE `{identity_column}` = {row[primary_key_index]}"
                logging.logger.info(f"Executing update query: {update_query}")
                target_cursor.execute(update_query)
            else:
                logging.logger.info(
                    f"Inserting data into {table_name} with id {row[primary_key_index]}"
                )
                # Prepare the values for the insert statement
                insert_query = (
                    f"INSERT INTO `{table_name}` VALUES ({', '.join(values)})"
                )
                logging.logger.info(f"Executing insert query: {insert_query}")
                target_cursor.execute(insert_query)
    target_cnx.commit()


def format_row_data(row, columns_def):
    values = []
    columns = columns_def.keys()

    for value, column in zip(row, columns):
        column_type = columns_def[column]["type"]
        if value is None:
            values.append("NULL")
        elif isinstance(value, datetime.datetime) or isinstance(value, datetime.date):
            if column_type in ["DATE", "DATETIME", "TIMESTAMP"]:
                values.append(f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'")
            elif column_type in ["TIME"]:
                values.append(f"'{value.strftime('%H:%M:%S')}'")
            elif column_type == "YEAR":
                values.append(f"'{value.year}'")
        elif isinstance(value, decimal.Decimal):
            values.append(str(value))
        elif isinstance(value, str):
            values.append(f"'{escape_string(value)}'")
        else:
            values.append(str(value))
    return values


def get_tables():
    global source_cnx, target_cnx, source_cursor, target_cursor
    source_cursor.execute("SHOW TABLES")
    source_tables = source_cursor.fetchall()
    return source_tables


def get_stored_procedures():
    global source_cnx, target_cnx, source_cursor, target_cursor
    source_cursor.execute(f"SHOW PROCEDURE STATUS WHERE Db = '{source_cnx._database}'")
    source_procedures = source_cursor.fetchall()
    return source_procedures


def get_functions():
    global source_cnx, target_cnx, source_cursor, target_cursor
    source_cursor.execute(f"SHOW FUNCTION STATUS WHERE Db = '{source_cnx._database}'")
    source_functions = source_cursor.fetchall()
    return source_functions
