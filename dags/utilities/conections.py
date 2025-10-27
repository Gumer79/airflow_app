# -*- coding: UTF-8 -*-
import json

from airflow.sdk.bases.hook import BaseHook


def gcp_connection(conn_id: str) -> json:
    conn = BaseHook.get_connection(conn_id)
    return conn.get_extra_dejson()
    # print(conn.get_extra_dejson())
    # print(type(conn.get_extra_dejson()))
    # return json.loads(json.loads(conn.get_extra_dejson())["keyfile_dict"])


def get_db_connection_url(conn_id: str) -> str:
    connection = BaseHook.get_connection(conn_id)
    if not connection:
        raise Exception("There is no connection record with `%s` name!" % conn_id)
    return f"postgresql://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}"
