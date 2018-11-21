# -*- coding: UTF-8 -*-
# Author:Christina.Li
import sys

reload(sys)
sys.setdefaultencoding("utf8")

import ConfigParser
import os
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
    TableMapEvent
)
import json,time
from get_data import Params,get_data
from Tables import Tables


class from_sour:
    def __init__(self,dbcode):
        params = Params(dbcode)
        self.__dest_db = get_data(host=params.dest_host,user=params.dest_user,
                                  password=params.dest_password,database=params.dest_database)
        self.mysql_setting = params.config
        self.__process_tables = Tables()
        self.__mapped_table = self.__assemble_sql()
        EVENTS = [DeleteRowsEvent,WriteRowsEvent,UpdateRowsEvent]
        commit_logfile = "demo.rct"
        (self.__commit_file,log_file,log_pos) = self.__get_log_position(commit_logfile)
        self.__old_log_file = log_file
        self.__old_log_pos = log_pos

        if log_file == None:
            exit(1)

        self.stream = BinLogStreamReader(connection_settings=self.mysql_setting,
                                         only_events= EVENTS,
                                         log_file=log_file,
                                         log_pos=log_pos,
                                         server_id=params.server_id,
                                         resume_stream=True,
                                         only_schemas=params.only_schemas,
                                         blocking=True
                                         )


    def __get_log_position(self,commit_log):
        if not os.path.exists(commit_log):
            os.system(r'touch %s' % commit_log)

        commit_file = open(commit_log,'r+')
        line = commit_file.readlines()
        if len(line) == 1 and line[0].count(":") == 1:
            (log_file,log_pos) = line[0].split(":")
        else:
            msg = "Error: commit_log file "  + commit_log + " is error or no " \
                                "log_file:log_pos specified in commit log file"
            self.msg(msg)
            (log_file,log_pos) = (None,-1)
        return (commit_file,log_file,int(log_pos))

    def msg(self,msg):
        print msg
        sys.stdout.flush()

    def producer(self):
        for binlogevent in self.stream:
            try:
                if isinstance(binlogevent,TableMapEvent):
                    binlogevent.dump()
                else:
                    for row in binlogevent.rows:
                        event = {"schema":binlogevent.schema,"table":binlogevent.table}

                        if isinstance(binlogevent, DeleteRowsEvent):
                            event["action"] = "delete"
                            event["values"] = dict(row["values"].items())
                            event = dict(event.items())
                        elif isinstance(binlogevent, UpdateRowsEvent):
                            event["action"] = "update"
                            event["before_values"] = dict(row["before_values"].items())
                            event["values"] = dict(row["after_values"].items())
                            event = dict(event.items())
                        elif isinstance(binlogevent,WriteRowsEvent):
                            event["action"] = "insert"
                            event["values"] = dict(row["values"].items())
                            event = dict(event.items())

                        event["timestamp"] = binlogevent.timestamp
                        event={key.lower():value for key,value in event.items()}
                        event["values"] = {key.lower():value for key,value in event['values'].items()}

                        timeArray = time.localtime(event["timestamp"])
                        otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)

                        print otherStyleTime,self.stream.log_file,self.stream.log_pos,binlogevent.table,event["action"]
                        print event["values"]
                        # sys.stdout.flush()
                        # self.__process_msg(event)

            except Exception,e:
                self.msg(str(e))
                self.msg(binlogevent.dump())

    def close(self):
        self.msg("stop streaming...")
        self.stream.close()
        exit(1)

    def __process_msg(self,event):
        if self.__mapped_table.has_key(event['table']):
            self.__process_normal(event)
        else:
            process = getattr(Tables,event['table'])
            print "ya"

    def __process_normal(self,event):
        action = event['action']
        table = event['table']
        try:
            for result in self.__mapped_table[table][action]:
                nwq_val = []
                for val in result['val']:
                    value = event["values"][val]
                    nwq_val.append(value)
                print result['sql'],nwq_val
                self.__dest_db.execute(result['sql'],nwq_val)
            self.__write_commit_log(self.stream.log_file,self.stream.log_pos)
            self.__dest_db.commit()

        except Exception,e:
            self.msg(e)
            self.msg(event)

    def __write_commit_log(self,log_file,log_pos):
        self.__commit_file.seek(0)
        self.__commit_file.writelines(log_file+":"+str(log_pos)+" "*20)
        self.__commit_file.close()
        self.__commit_file = open(self.__commit_file.name,"r+")




    def __assemble_sql(self):
        mapped_conf = ConfigParser.ConfigParser()
        map_file = "tables.conf"
        if os.path.exists(map_file):
            mapped_conf.read(map_file)
        else:
            return {}
        assemble = {}
        for table in mapped_conf.sections():
            table = table.strip()
            assemble[table] = {}
            assemble[table]['delete'] = []
            assemble[table]['insert'] = []
            assemble[table]['update'] = []

            source_pk = mapped_conf.get(table,"source_pk").lower().split(",")
            dest_pk = mapped_conf.get(table, "dest_pk").lower().split(",")
            # print dest_pk
            sql_del = "delete from " + table + " where "
            for i in range(len(dest_pk)):
                sql_del = sql_del + dest_pk[i] + "=%s and "
            sql_del = sql_del.strip()[:-3]
            assemble[table]['delete'].append({'sql': sql_del, 'val': source_pk})
            mapped_fields = mapped_conf.items(table)
            sql_insert = "insert into " + table + " ("
            for i in range(len(dest_pk)):
                sql_insert = sql_insert + dest_pk[i] + ","
            val_insert = source_pk[:]
            sql_update = "update " + table + " set "
            val_update = []
            for (field, mapped_field) in mapped_fields:
                if field != 'source_pk' and field != 'dest_pk':
                    sql_insert = sql_insert + field + ','
                    sql_update = sql_update + field + "=%s, "

                    val_insert.append(mapped_field.lower())
                    val_update.append(mapped_field.lower())
            sql_insert = sql_insert[:-1] + ") values (" + ("%s, " * len(val_insert)).strip()[:-1] + ")"
            sql_update = sql_update.strip()[:-1] + " where "
            for i in range(len(dest_pk)):
                sql_update = sql_update + dest_pk[i] + "=%s and "
            sql_update = sql_update.strip()[:-3]
            for i in range(len(source_pk)):
                val_update.append(source_pk[i])
            assemble[table]['insert'].append({'sql': sql_insert, 'val': val_insert})
            assemble[table]['update'].append({'sql': sql_update, 'val': val_update})
        return assemble











if __name__ == '__main__':
    test = from_sour("localhost")
    test.producer()
    test.close()






























































if __name__ == '__main__':
    # r = redis.Redis()
    stream = BinLogStreamReader(
        connection_settings=Mysql_Setting,
        server_id=1,
        only_events=[DeleteRowsEvent,WriteRowsEvent,UpdateRowsEvent]
    )

    for binlogevent in stream:

        for row in binlogevent.rows:
            event = {"schema":binlogevent.schema,
                     "table":binlogevent.table
                     }
            if isinstance(binlogevent,DeleteRowsEvent):
                event["action"] = "delete"
                event["values"] = dict(row["values"].items())
                event = dict(event.items())
                # print vals
            elif isinstance(binlogevent,UpdateRowsEvent):
                event["action"] = "update"
                event["before_values"] = dict(row["before_values"].items())
                event["values"] = dict(row["after_values"].items())
                event = dict(event.items())
                # print vals
            elif isinstance(binlogevent,WriteRowsEvent):
                event["action"] = "insert"
                event["values"] = dict(row["values"].items())
                event = dict(event.items())

            event['timestamp'] = binlogevent.timestamp
            event = {key.lower(): value for key, value in event.items()}
            event["values"] = {key.lower():value for key,value in event["values"].items()}

            timeArray = time.localtime(event["timestamp"])
            otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)

            print otherStyleTime, stream.log_file,stream.log_pos,binlogevent.table,\
                event["action"],event['values']
            sys.stdout.flush()
            # print json.dumps(event)
            # sys.stdout.flush()


    stream.close()





















































