from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_redshift_schema(hook, schema, table):
    df = hook.get_pandas_df("""SELECT column_name
	FROM information_schema.columns
	WHERE table_schema = '{schema}' and table_name = '{table}'
	ORDER BY ordinal_position""".format(schema=schema, table=table))
    ret = []
    for index, row in df.iterrows():
        ret.append(row["column_name"])
    return ret
