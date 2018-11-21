# -*- coding: UTF-8 -*-
# Author:Christina.Li
import sys

reload(sys)
sys.setdefaultencoding("utf8")

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent
)
import sys
import json

Mysql_Setting={
    "host": '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'passwd': '123456'
}

if __name__ == '__main__':
    stream = BinLogStreamReader(
        connection_settings=Mysql_Setting,
        server_id=5,
        blocking=True,
        only_schemas=['lcc'],
        only_events=[
            DeleteRowsEvent,WriteRowsEvent,UpdateRowsEvent
        ]
    )

    for binlogevent in stream:
        for row in binlogevent.rows:
            event = {
                "schema":binlogevent.schema, "table":binlogevent.table
            }
            if isinstance(binlogevent,DeleteRowsEvent):
                event["action"] = "delete"
                event["values"] = dict(row["values"].items())
                event = dict(event.items())
            elif isinstance(binlogevent,UpdateRowsEvent):
                event["action"] = "update"
                event["before_values"] = dict(row["before_values"].items())
                event["after_values"] = dict(row["after_values"].items())
                event = dict(event.items())
            elif isinstance(binlogevent,WriteRowsEvent):
                event["action"] = "insert"
                event["values"] = dict(row["values"].items())
                event = dict(event.items())
            print json.dumps(event)
            sys.stdout.flush()

    stream.close()





































































