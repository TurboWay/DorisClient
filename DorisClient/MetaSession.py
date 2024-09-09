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
from .BaseSession import DorisSession, Logger
from ._BaseSql import MetaSql, MetaSql_tablets, MetaDDL_Table, MetaDDL_Tablet, MetaDDL_Partition, MetaDDL_Size, MetaDDL_Table_Count, MetaDDL_Materialized_View

log = Logger(name=__name__)

class DorisMeta(DorisSession):
    """
    Recycle `show xxx from table` for each table to collect metadata
    """

    def _delete_sql(self, meta_table, **kwargs):
        """
        return (delete_sql, filter)
        """
        if any(kwargs.values()):
            filter = ' and '.join([f"{k}='{v}'" for k, v in kwargs.items() if v])
            return f"delete from {meta_table} where {filter}", filter
        else:
            return f'truncate table {meta_table}', ''

    def _base(self, **kwargs):
        """
        return base meta for collect
        """
        collect_type = kwargs.get('collect_type')
        filter = kwargs.get('filter')
        sql = MetaSql_tablets if collect_type == 'tablets_sql' else MetaSql
        if filter:
            sql = sql.replace('1=1', filter)
        return self.read(sql)

    def create_tables(self):
        self.execute(MetaDDL_Table)
        self.execute(MetaDDL_Partition)
        self.execute(MetaDDL_Tablet)
        self.execute(MetaDDL_Size)
        self.execute(MetaDDL_Table_Count)
        self.execute(MetaDDL_Materialized_View)

    def collect_table(self, meta_table='meta_table', ignore_view=True, **kwargs):
        """
        use `show create table xxx` collect meta  ==> meta_table
        param **kwargs:
            database_name    filter condition, default None
            table_name       filter condition, default None
        """
        delete_sql, filter = self._delete_sql(meta_table, **kwargs)
        data = []
        for row in self._base(filter=filter):
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
                    bucket_num = re.findall('BUCKETS (\d+)', ddl)
                    properties = re.findall('PROPERTIES \((.*)\)', ddl, re.S)
                    properties = '{' + properties[0].replace(' = ', ':').replace('\n', '') + '}' if properties else '{}'
                    replication_num = re.findall('tag\.location\.[^,]+: (\d+)', eval(properties).get('replication_allocation'))
                    row.update({
                        'engine': engine[0] if engine else '',
                        'model': model[0] if model else '',
                        'replication_num': sum([int(i) for i in replication_num]),
                        'bucket_num': bucket_num[0] if bucket_num else 0,
                        'properties': properties
                    })
            except Exception as e:
                log.warning(f"{row['database_name']}.{row['table_name']} meta error, {e}")
            finally:
                data.append(row)
        if data:
            self.execute(delete_sql)
            self.streamload(meta_table, data)

    def _collect(self, meta_table, collect_type, **kwargs):
        delete_sql, filter = self._delete_sql(meta_table, **kwargs)
        self.execute(delete_sql)
        data = []
        for row in self._base(collect_type=collect_type, filter=filter):
            database_name, table_name, sql = row['database_name'], row['table_name'], row[collect_type]
            if not sql:
                continue
            try:
                items = self.read(sql)
                for item in items:
                    item['database_name'] = database_name
                    item['table_name'] = table_name
                    if item.get('LocalDataSize'):
                        item['DataSize'] = item.get('LocalDataSize')  # meta key rename since 1.2
                    if collect_type == 'tablets_sql':
                        item['PartitionId'] = row.get('PartitionId')
                        item['PartitionName'] = row.get('PartitionName')
                    item['update_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    data.append(item)
            except Exception as e:
                log.warning(f"{database_name}.{table_name} meta error, {e}")
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

    def collect_tablet(self, meta_table='meta_tablet', **kwargs):
        """
        use `show tablets from xxx partition xxx` collect meta  ==> meta_tablet
        param **kwargs:
            database_name    filter condition, default None
            table_name       filter condition, default None
        """
        self._collect(meta_table, 'tablets_sql', **kwargs)

    def collect_partition(self, meta_table='meta_partition', **kwargs):
        """
        use `show partitio from xxx` collect meta  ==> meta_partition
        param **kwargs:
            database_name    filter condition, default None
            table_name       filter condition, default None
        """
        self._collect(meta_table, 'partitions_sql', **kwargs)

    def _tobyte(self, i):
        size_dict = {
            'KB': 1024, 'MB': 1024 ** 2, 'GB': 1024 ** 3, 'TB': 1024 ** 4, 'PB': 1024 ** 5
        }
        for key, val in size_dict.items():
            if i.endswith(key):
                return float(i.split(' ')[0]) * val
        else:
            return i

    def collect_size(self, meta_table='meta_size', **kwargs):
        """
        use `show data` collect meta  ==> meta_size
        param **kwargs:
            database_name    filter condition, default None
            table_name       filter condition, default None
        """
        delete_sql, _ = self._delete_sql(meta_table, **kwargs)
        database_name = kwargs.get('database_name', '')
        filter = f" and schema_name='{database_name}'" if database_name else ''
        sql = f"select schema_name as schema_name from information_schema.schemata where schema_name <> 'information_schema' {filter} order by 1"
        rows = self.read(sql)
        data = []
        for row in rows:
            database_name = row['schema_name']
            self.execute(f'use {database_name}')
            items = self.read('show data')
            for item in items:
                if item['TableName'] not in ('Total', 'Quota', 'Left'):
                    if kwargs.get('table_name') and item['TableName'] != kwargs.get('table_name'):
                        continue
                    item['database_name'] = database_name
                    item['table_name'] = item.pop('TableName')
                    item['update_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    item['SizeByte'] = self._tobyte(item['Size'])
                    data.append(item)
        if data:
            self.execute(f'use {self.database}')
            self.execute(delete_sql)
            self.streamload(meta_table, data)

    def collect_table_count(self, meta_table='meta_table_count', **kwargs):
        """
        select count(1) from table  ==> meta_table_count
        param **kwargs:
            database_name    filter condition, default None
        """
        delete_sql, _ = self._delete_sql(meta_table, **kwargs)
        filter = f"and table_schema='{kwargs.get('database_name')}'" if kwargs.get('database_name') else ''
        self.execute(delete_sql)
        sql = f"""
        select table_schema, table_name
        from information_schema.tables 
        where table_type = 'BASE TABLE' 
        and `ENGINE` = 'Doris'
        and table_schema not in ('__internal_schema') {filter}
        order by table_schema, table_name
        limit 10000000
        """
        rows = self.read(sql)
        items = []
        for px, row in enumerate(rows, 1):
            table_schema, table_name = row['table_schema'], row['table_name']
            sql = f"""
            select a.table_schema as database_name
            ,a.table_name as table_name
            ,a.table_rows as table_rows
            ,s.real_table_rows as real_table_rows
            ,date_format(now(), '%Y-%m-%d %H:%i:%S') as update_time 
            from information_schema.tables a
            cross join (select count(1) as real_table_rows from {table_schema}.`{table_name}`) s
            where a.table_schema = '{table_schema}'
            and a.table_name = '{table_name}'
            """
            items += self.read(sql)
            log.info(f"【{px}/{len(rows)}】{table_schema}.{table_name} count success")
        self.streamload(meta_table, items)

    def collect_materialized_view(self, meta_table='meta_materialized_view', only_insert=False, **kwargs):
        """
        use `desc tb all` + `show create materialized view xx on tb`
           ==> materialized_view
        param **kwargs:
            database_name    filter condition, default None
        """
        delete_sql, _ = self._delete_sql(meta_table, **kwargs)
        filter = f"and table_schema='{kwargs.get('database_name')}'" if kwargs.get('database_name') else ''
        if not only_insert:
            self.execute(delete_sql)
        sql = f"""
        select table_schema, table_name
        from information_schema.tables 
        where table_type = 'BASE TABLE' 
        and `ENGINE` = 'Doris'
        and table_schema not in ('__internal_schema') {filter}
        order by table_schema, table_name
        limit 10000000
        """
        rows = self.read(sql)
        items = []
        for px, row in enumerate(rows, 1):
            table_schema, table_name = row['table_schema'], row['table_name']
            sql = f"desc `{table_schema}`.`{table_name}` all"
            for row in self.read(sql):
                view_name = row['IndexName']
                if view_name and row['IndexName'] != table_name and row['IndexKeysType'] == 'AGG_KEYS':
                    show_sql = f'show create materialized view {view_name} on `{table_schema}`.`{table_name}`'
                    rows = self.read(show_sql)
                    if rows:
                        item = {
                            'database_name': table_schema,
                            'table_name': table_name,
                            'view_name' : view_name,
                            'ddl': rows[0]['CreateStmt'],
                            'update_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                        }
                        items.append(item)
        self.streamload(meta_table, items)