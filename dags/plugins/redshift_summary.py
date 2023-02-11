from airflow import DAG
from airflow.macros import *

from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

import logging
from glob import glob
"""
Build a summary table under analytics schema in Redshift
- Check the input tables readiness first
- Run the SQL using a temp table
- Before swapping, check the size of the temp table
- Finally swap
"""
def load_all_jsons_into_list(path_to_json):

    configs = []
    for f_name in glob(path_to_json+ '/*.py'):
        # logging.info(f_name)
        with open(f_name) as f:
            dict_text = f.read()
            try:
                dict = eval(dict_text)
            except Exception as e:
                logging.info(str(e))
                raise
            else:
                configs.append(dict)

    return configs


def find(table_name, table_confs):
    """
    scan through table_confs and see if there is a table matching table_name
    """
    for table in table_confs:
        if table.get("table") == table_name:
            return table

    return None


def build_summary_table(dag_root_path, dag, tables_load, redshift_conn_id, start_task=None):
    logging.info(dag_root_path)
    table_confs = load_all_jsons_into_list(dag_root_path + "/config/")

    if start_task is not None:
        prev_task = start_task
    else:
        prev_task = None

    for table_name in tables_load:

        table = find(table_name, table_confs)
        summarizer = RedshiftSummaryOperator(
            table=table["table"],
            schema=table["schema"],
            redshift_conn_id=redshift_conn_id,
            input_check=table["input_check"],
            main_sql=table["main_sql"],
            output_check=table["output_check"],
            overwrite=table.get("overwrite", True),
            after_sql=table.get("after_sql"),
            pre_sql=table.get("pre_sql"),
            attributes=table.get("attributes", ""),
            dag=dag,
            task_id="anayltics"+"__"+table["table"]
        )
        if prev_task is not None:
            prev_task >> summarizer
        prev_task = summarizer
    return prev_task


def redshift_sql_function(**context):
    """this is a main Python callable function which runs a given SQL
    """

    sql=context["params"]["sql"]
    print(sql)
    hook = PostgresHook(postgres_conn_id=context["params"]["redshift_conn_id"])
    hook.run(sql, True)


class RedshiftSummaryOperator(PythonOperator):
    """
    Create a summary table in Redshift
    :param input_check: a list of input tables to check to make sure
                        they are fully populated. the list is composed
                        of sql (select) and minimum count
    :type input_check: a list of sql and count
    :param main_sql: a main sql to create a summary table. this should
                     use a temp table. this sql can have more than one 
                     statement
    :type main_sql: string
    :input output_check: output validation. It is a list of sql (select)
                         and minimum count
    :type output_check: a list of sql and count
    :input overwrite: Currently this only supports overwritting (True)
                      Once False is supported, it will append to the table
    :type overwrite: boolean
    """

    @apply_defaults
    def __init__(self,
                 schema,
                 table,
                 redshift_conn_id,
                 input_check,
                 main_sql,
                 output_check,
                 overwrite,
                 params={},
                 pre_sql="",
                 after_sql="",
                 attributes="",
                 *args,
                 **kwargs
                 ):
        self.schema = schema
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.input_check = input_check
        self.main_sql = main_sql
        self.output_check = output_check

        # compose temp table creation, insert into the temp table as params
        if pre_sql:
            main_sql = pre_sql
            if not main_sql.endswith(";"):
                main_sql += ";"
        else:
            main_sql = ""

        main_sql += "DROP TABLE IF EXISTS {schema}.temp_{table};".format(
            schema=self.schema,
            table=self.table
        )
        # now we are using "CREATE TABLE ... AS SELECT" syntax
        # we used to create a temp table with the same schema as the main table and then insert into the temp table 
        main_sql += "CREATE TABLE {schema}.temp_{table} {attributes} AS ".format(
            schema=self.schema,
            table=self.table,
            attributes=attributes
        ) + self.main_sql

        if after_sql:
            self.after_sql = after_sql.format(
                schema=self.schema,
                table=self.table
            )
        else:
            self.after_sql = ""

        super(RedshiftSummaryOperator, self).__init__(
            python_callable=redshift_sql_function,
            params={
                "sql": main_sql,
                "overwrite": overwrite,
                "redshift_conn_id": self.redshift_conn_id
            },
            provide_context=True,
            *args,
            **kwargs
        )

    def swap(self):
        sql = """BEGIN;
        DROP TABLE IF EXISTS {schema}.{table} CASCADE;
        ALTER TABLE {schema}.temp_{table} RENAME TO {table};   
        GRANT SELECT ON TABLE {schema}.{table} TO GROUP analytics_users;
        END
        """.format(schema=self.schema,table=self.table)
        self.hook.run(sql, True)

    def execute(self, context):
        """Do input_check first
        - input_check should be a list of dictionaries
        - each item in the dictionary contains "sql" and "count"
        """
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for item in self.input_check:
            (cnt,) = self.hook.get_first(item["sql"])
            if cnt < item["count"]:
                raise AirflowException(
                    "Input Validation Failed for " + str(item["sql"]))

        """
        - create a temp table using create table like
        - run insert into the temp table
        """
        return_value = super(RedshiftSummaryOperator, self).execute(context)

        """Do output_check using self.output_check
        """
        for item in self.output_check:
            (cnt,) = self.hook.get_first(item["sql"].format(schema=self.schema, table=self.table))
            if item.get("op") == 'eq':
                if int(cnt) != int(item["count"]):
                    raise AirflowException(
                        "Output Validation of 'eq' Failed for " + str(item["sql"]) + ": " + str(cnt) + " vs. " + str(item["count"])
                    )
            else:
                if cnt < item["count"]:
                    raise AirflowException(
                        "Output Validation Failed for " + str(item["sql"]) + ": " + str(cnt) + " vs. " + str(item["count"])
                    )
        """Now swap the temp table name
        """
        self.swap()

        if self.after_sql:
            self.hook.run(self.after_sql, True)

        return return_value
