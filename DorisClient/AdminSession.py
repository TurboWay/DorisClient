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
import math
from .BaseSession import DorisSession, Logger

log = Logger(name=__name__)

class DorisAdmin(DorisSession):

    def _tobyte(self, i):
        size_dict = {
            'KB': 1024, 'MB': 1024 ** 2, 'GB': 1024 ** 3, 'TB': 1024 ** 4, 'PB': 1024 ** 5
        }
        for key, val in size_dict.items():
            if i.endswith(key):
                return float(i.split(' ')[0]) * val
        else:
            return i

    def get_buckets(self, database_name, table_name):
        size = self.read(f'show data from {database_name}.{table_name}')[0]['Size']
        buckets = math.ceil(self._tobyte(size) / 524288000)  # Add one bucket for every 500M
        return buckets

    def check(self, log_name, table_name, partition_name=None):
        if partition_name:
            tb = f'{table_name} partition {partition_name}'
            tb_tmp = f'{table_name} temporary partition {partition_name}_tmp'
        else:
            tb, tb_tmp = table_name, f'{table_name}_tmp'
        sql = f"""
        select distinct ct
        from(
        select count(1) as ct from {tb}
        union all select count(1) as ct from {tb_tmp}
        ) s 
        """
        if len(self.read(sql)) == 1:
            log.info(f'【{log_name}】check pass ...')
        else:
            log.error(f'【{log_name}】check fail \n{sql} !!!')
            raise Exception


    def modify(self, **kwargs):
        """
        Modify the number and method of buckets for the specified table or partition

        function setp:
            whole table:  1.create new table >> 2.insert into new table >> 3.replace table with new table
            single partition: 1.create temp_partition >> 2.insert into temp_partition from partition >> 3.replace partition with the temp_partition

        param **kwargs:
            database_name
            table_name
            partition_name       default none, change table by default
            distribution_key     default none, change nothing by default
            buckets              default none, use self.get_buckets to calculate a number
        """
        database_name = kwargs.get('database_name')
        table_name = kwargs.get('table_name')
        partition_name = kwargs.get('partition_name')
        distribution_key = kwargs.get('distribution_key', '').replace('`','').strip()
        buckets = kwargs.get('buckets')
        assert all([database_name, table_name]), '`database_name` and `table_name` cannot be empty !!!'
        if buckets:
            assert isinstance(buckets, int), '`buckets` only accept int value !!!'
        # get old config
        self.execute(f'use {database_name};')
        ddl = self.read(f"show create table {table_name}")[0]['Create Table']
        old_distribution_key = re.findall('DISTRIBUTED BY (.*?) BUCKETS', ddl)[0]
        old_distribution_key = re.sub('`|\(|\)|HASH| ', '', old_distribution_key)
        old_buckets = re.findall('BUCKETS (\d+)', ddl)[0]
        if partition_name:
            rows = self.read(f"show partitions from {table_name} where PartitionName='{partition_name}'")
            if rows:
                row = rows[0]
                old_distribution_key, old_buckets = row['DistributionKey'], row['Buckets']
            else:
                log.warning(f"Partition {partition_name} does not exist !")
                return

        # check diff
        buckets = buckets if buckets else self.get_buckets(database_name, table_name)
        if distribution_key.upper() == old_distribution_key.upper() and buckets == int(old_buckets):
            log.warning("nothing changed !")
            return
        if partition_name:
            if distribution_key and distribution_key.upper() != old_distribution_key.upper():
                log.warning("Partition does not support modifying the bucketing key !!!")
            if buckets == int(old_buckets):
                log.warning("nothing changed !")
                return

        # alter operation
        log_name = f'{table_name} [{partition_name}]' if partition_name else table_name
        old_distribution_key = f"HASH({old_distribution_key})" if not re.findall('HASH|RANDOM',old_distribution_key) else old_distribution_key
        if not partition_name:
            """
            whole table
            """
            if distribution_key.upper() == 'RANDOM':
                distribution_key = 'RANDOM'
            elif distribution_key:
                distribution_key = f'HASH({distribution_key})'
            else:
                distribution_key = old_distribution_key
            tmp_tb = f'{table_name}_tmp'
            tmp_ddl = ddl.replace(table_name, tmp_tb)
            tmp_ddl = re.sub('DISTRIBUTED BY .*? BUCKETS \d+', f'DISTRIBUTED BY {distribution_key} BUCKETS {buckets}',tmp_ddl)
            tmp_ddl = re.sub('"dynamic_partition.buckets" = "\d+"', f'"dynamic_partition.buckets" = "{buckets}"',tmp_ddl)
            # 1.create new table
            log.info(f'【{log_name}】create table {tmp_tb} ...')
            self.execute(tmp_ddl)
            # 2.insert into new table
            insert_sql = f'insert into {tmp_tb} select * from {table_name};'
            log.info(f'【{log_name}】insert into {tmp_tb} ...')
            self.execute(insert_sql)
            log.info(f'【{log_name}】check the number of records in two tables ...')
            self.check(log_name, table_name)
            # 3.replace table with new table
            log.info(f'【{log_name}】replace {table_name} with {tmp_tb}...')
            replace_sql = f"alter table {table_name} replace with table {tmp_tb} properties('swap' = 'false');"
            self.execute(replace_sql)
            log.info(f'【{log_name}】({old_distribution_key} {old_buckets}) >> ({distribution_key} {buckets})')
            log.info(f'【{log_name}】changed success')
        else:
            """
            single partition
            """
            tmp_partition = f"{partition_name}_tmp"
            partition_values = re.findall(f'PARTITION {partition_name} (VALUES .*)[,|\)|\]]', ddl)[0].replace(partition_name,tmp_partition)
            # 1.create temp_partition
            tmp_ddl = f"alter table {table_name} add temporary partition {tmp_partition} {partition_values} DISTRIBUTED BY {old_distribution_key} BUCKETS {buckets}"
            log.info(f'【{log_name}】create temp_partition {tmp_partition} ...')
            self.execute(tmp_ddl)
            # 2.insert into temp_partition from partition
            log.info(f'【{log_name}】insert into {tmp_partition} ...')
            insert_sql = f"insert into {table_name} temporary partition {tmp_partition} select * from {table_name} partition {partition_name}"
            self.execute(insert_sql)
            log.info(f'【{log_name}】check the number of records in two partitions ...')
            self.check(log_name, table_name, partition_name)
            # 3.replace partition with the temp_partition
            replace_sql = f"alter table {table_name} replace partition {partition_name} with temporary partition {tmp_partition};"
            self.execute(replace_sql)
            log.info(f'【{log_name}】({old_distribution_key} {old_buckets}) >> ({old_distribution_key} {buckets})')
            log.info(f'【{log_name}】changed success')
