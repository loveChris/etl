# -*- coding: UTF-8 -*-
# Author:Christina.Li
import sys

reload(sys)
sys.setdefaultencoding("utf8")

import ConfigParser,os

def test():
    mapped_conf = ConfigParser.ConfigParser()
    map_file = "test.conf"
    if os.path.exists(map_file):
        mapped_conf.read(map_file)
    else:
        print "no config file"

    assemble = {}
    for table in mapped_conf.sections():
        table = table.strip()
        if table.find(".")==-1:
            assemble[table] = {}
            assemble[table]['delete'] = []
            assemble[table]['insert'] = []
            assemble[table]['update'] = []

            if mapped_conf.has_option(table,"map"):
                mapped_tables = mapped_conf.get(table, "map").split(",")
            else:
                continue
            for mapped_table in mapped_tables:
                mapped_table = mapped_table.strip()
                source_pk = mapped_conf.get(mapped_table,"source_pk").lower().split(",")
                dest_pk = mapped_conf.get(mapped_table,"dest_pk").lower()
                bind = ("%s,"*len(dest_pk.split(","))).strip()[:-1]
                sql_del = "delete from " + mapped_table + " where (" + dest_pk + ") = (" + bind + ")"
                assemble[table]['delete'].append({'sql':sql_del,'val':source_pk})
                mapped_fields = mapped_conf.items(mapped_table)
                sql_insert = "insert into "+mapped_table+" ("+dest_pk+","
                val_insert = source_pk[:]
                sql_update = "update " + mapped_table + " set "
                val_update = []
                # print mapped_fields
                for (field,mapped_field) in mapped_fields:
                    if field not in ['source_pk','dest_pk']:
                        sql_insert = sql_insert + field + ','
                        sql_update = sql_update + field + " =%s,"

                        if mapped_field.count(".") == 0:
                            val_insert.append(mapped_field.lower())
                            val_update.append(mapped_field.lower())
                        else:
                            val_insert.append(mapped_field)
                            val_update.append(mapped_field)

                sql_update = sql_update.strip()[:-1] + " where (" + dest_pk + ") = (" + bind + ")"

                for i in range(len(source_pk)):
                    val_update.append(source_pk[i])

                sql_insert = sql_insert[:-1] + ") values (" + ("%s,"*len(val_insert))[:-1] + ")"
                assemble[table]['insert'].append({'sql':sql_insert,'val':val_insert})
                assemble[table]['update'].append({'sql':sql_update,'val':val_update})
    return assemble






if __name__ == '__main__':
    a = test()
    # print a
    for i in a:
        for j in a[i]:
            print i
            print j
            print a[i][j][0]['val']
            print a[i][j][0]['sql']





