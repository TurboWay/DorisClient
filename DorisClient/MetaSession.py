#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import re
import time
from .BaseSession import DorisSession, DorisLogger
from ._BaseSql import MetaSql, MetaSql_tablets, MetaDDL_Table, MetaDDL_Tablet, MetaDDL_Partition, MetaDDL_Size


class DorisMeta(DorisSession):
    """
    Recycle `show xxx from table` for each table to collect metadata
    """

    def _base(self, collect_type=''):
        sql = MetaSql_tablets if collect_type == 'tablets_sql' else MetaSql
        return self.read(sql)

    def create_tables(self):
        self.execute(MetaDDL_Table)
        self.execute(MetaDDL_Partition)
        self.execute(MetaDDL_Tablet)
        self.execute(MetaDDL_Size)

    def collect_table(self, meta_table='meta_table', ignore_view=True):
        data = []
        for row in self._base():
            if ignore_view and row['table_type'] == 'VIEW':
                continue
            del row['partitions_sql'], row['tablets_sql']
            try:
                sql = row.pop('ddl_sql')
                ddl = self.read(sql, cursors=None)[0][1]
                row['ddl'] = ddl
                row['update_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                if sql.startswith('show create table'):
                    engine = re.findall('ENGINE=(.*)', ddl)
                    model = re.findall('(.*) KEY\(', ddl)
                    replication_num = re.findall('replication_allocation.*(\d+)', ddl)
                    bucket_num = re.findall('BUCKETS (\d+)', ddl)
                    properties = re.findall('PROPERTIES \((.*)\)', ddl, re.S)
                    row.update({
                        'engine': engine[0] if engine else '',
                        'model': model[0] if model else '',
                        'replication_num': replication_num[0] if replication_num else 0,
                        'bucket_num': bucket_num[0] if bucket_num else 0,
                        'properties': '{' + properties[0].replace(' = ', ':').replace('\n',
                                                                                      '') + '}' if properties else ''
                    })
            except Exception as e:
                DorisLogger.warning(f"{row['database_name']}.{row['table_name']} meta error, {e}")
            finally:
                data.append(row)
        if data:
            self.execute(f'truncate table {meta_table}')
            self.streamload(meta_table, data)

    def _collect(self, meta_table, collect_type):
        self.execute(f'truncate table {meta_table}')
        data = []
        for row in self._base(collect_type):
            database_name, table_name, sql = row['database_name'], row['table_name'], row[collect_type]
            if not sql:
                continue
            try:
                items = self.read(sql)
                for item in items:
                    item['database_name'] = database_name
                    item['table_name'] = table_name
                    if not item.get('DataSize'):
                        item['DataSize'] = item.get('localDataSize')  # meta key rename since 1.2
                    if collect_type == 'tablets_sql':
                        item['PartitionId'] = row.get('PartitionId')
                    item['update_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    data.append(item)
            except Exception as e:
                DorisLogger.warning(f"{database_name}.{table_name} meta error, {e}")
            finally:
                if len(data) >= 30000:
                    if self.streamload(meta_table, data):
                        data.clear()
                    else:
                        raise Exception("streamload error !!!")
        if data:
            if self.streamload(meta_table, data):
                ...
            else:
                raise Exception("streamload error !!!")

    def collect_tablet(self, meta_table='meta_tablet'):
        self._collect(meta_table, 'tablets_sql')

    def collect_partition(self, meta_table='meta_partition'):
        self._collect(meta_table, 'partitions_sql')

    def _tobyte(self, i):
        size_dict = {
            'KB': 1024, 'MB': 1024 ** 2, 'GB': 1024 ** 3, 'TB': 1024 ** 4, 'PB': 1024 ** 5
        }
        for key, val in size_dict.items():
            if i.endswith(key):
                return float(i.split(' ')[0]) * val
        else:
            return i

    def collect_size(self, meta_table='meta_size'):
        sql = "select schema_name from information_schema.schemata where schema_name <> 'information_schema' order by 1"
        rows = self.read(sql)
        data = []
        for row in rows:
            database_name = row['schema_name']
            self.execute(f'use {database_name}')
            items = self.read('show data')
            for item in items:
                if item['TableName'] not in ('Total', 'Quota', 'Left'):
                    item['database_name'] = database_name
                    item['table_name'] = item.pop('TableName')
                    item['update_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    item['SizeByte'] = self._tobyte(item['Size'])
                    data.append(item)
        if data:
            self.execute(f'use {self.database}')
            self.execute(f'truncate table {meta_table}')
            self.streamload(meta_table, data)
