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

import base64
import json
import time

import pymysql
import requests
import logging


def Logger(name=__name__, filename=None, level='INFO', filemode='a'):
    """
    :param name:
    :param filename: filename string
    :param level:
    :param filemode:
    :return:
    """
    logging.basicConfig(
        filename=filename,
        filemode=filemode,
        level=level,
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger(name)
    if filename:
        formatter = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s: %(message)s', "%Y-%m-%d %H:%M:%S")
        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    return logger


DorisLogger = Logger(name='DorisClient')


def Retry(*args, **kwargs):
    max_retry = kwargs.get('max_retry', 3)
    retry_diff_seconds = kwargs.get('retry_diff_seconds', 3)

    def warpp(func):
        def run(*args, **kwargs):
            for i in range(max_retry + 1):
                if i > 0:
                    DorisLogger.warning(f"will retry after {retry_diff_seconds} seconds，retry times : {i}/{max_retry}")
                time.sleep(retry_diff_seconds)
                flag = func(*args, **kwargs)
                if flag:
                    return flag

        return run

    return warpp


class DorisSession:

    def __init__(self, fe_servers, database, user, passwd, mysql_port=9030):
        """
        :param fe_servers: fe servers list, like: ['127.0.0.1:8030', '127.0.0.2:8030', '127.0.0.3:8030']
        :param database:
        :param user:
        :param passwd:
        :param mysql_port: port for sql client, default:9030
        """
        assert fe_servers
        assert database
        assert user
        assert passwd
        self.fe_servers = fe_servers
        self.database = database
        self.Authorization = base64.b64encode((user + ':' + passwd).encode('utf-8')).decode('utf-8')
        self.mysql_cfg = {
            'host': fe_servers[0].split(':')[0],
            'port': mysql_port,
            'database': database,
            'user': user,
            'passwd': passwd
        }
        self.conn = None

    def _connect(self):
        if self.conn is None:
            self.conn = pymysql.connect(**self.mysql_cfg)

    def _label(self, table):
        return f"{table}_{time.strftime('%Y%m%d_%H%M%S', time.localtime())}"

    def _columns(self, keys):
        return ','.join([f'`{column}`' for column in keys])

    def _get_be(self, table, headers):
        for fe_server in self.fe_servers:
            host, port = fe_server.split(':')
            url = f'http://{host}:{port}/api/{self.database}/{table}/_stream_load'
            response = requests.put(url, '', headers=headers, allow_redirects=False)
            if response.status_code == 307:
                return response.headers['Location']
        else:
            raise Exception("No available BE nodes can be obtained. Please check configuration")

    def _streamload(self, table, dict_array, **kwargs):
        assert isinstance(dict_array, list), 'TypeError: dict_array must be list'
        if not dict_array:
            DorisLogger.warning(f"Nothing has been send, because dict_array was empty")
            return True

        headers = {
            'Expect': '100-continue',
            'Authorization': 'Basic ' + self.Authorization,
            'format': 'json',
            'strip_outer_array': 'true',
            'fuzzy_parse': 'true',
        }
        if kwargs.get('sequence_col'):
            headers['function_column.sequence_col'] = kwargs.get('sequence_col')
        if kwargs.get('merge_type'):
            headers['merge_type'] = kwargs.get('merge_type')
        if kwargs.get('delete'):
            headers['delete'] = kwargs.get('delete')

        url = self._get_be(table, headers)
        headers['label'] = self._label(table)
        headers['columns'] = self._columns(dict_array[0].keys())
        response = requests.put(url, json.dumps(dict_array), headers=headers, allow_redirects=False)
        if response.status_code == 200:
            res = response.json()
            if res.get('Status') == 'Success':
                DorisLogger.info(res)
                return True
            elif res.get('Status') == 'Publish Timeout':
                DorisLogger.warning(res)
                return True
            else:
                DorisLogger.error(res)
                return False
        else:
            DorisLogger.error(response.text)
            return False

    @Retry(max_retry=3, retry_diff_seconds=3)
    def streamload(self, table, dict_array, **kwargs):
        # document >> https://github.com/TurboWay/DorisClient
        """
        :param table: target table
        :param dict_array: dict list ,eg: [{col1:val1}, {col2:val2}]
        :param kwargs:
             merge_type：APPEND，DELETE，MERGE
             delete：Only meaningful under MERGE, indicating the deletion condition of the data function_column.
             sequence_col: Only applicable to UNIQUE_KEYS. Under the same key column,
                           ensure that the value column is REPLACEed according to the source_sequence column.
        :return:
        """
        self._streamload(table, dict_array, **kwargs)

    def execute(self, sql, args=None):
        self._connect()
        with self.conn.cursor() as cur:
            cur.execute(sql, args)
            self.conn.commit()
        return True

    def read(self, sql, cursors=pymysql.cursors.DictCursor, args=None):
        self._connect()
        with self.conn.cursor(cursors) as cur:
            cur.execute(sql, args)
            return cur.fetchall()

    def __del__(self):
        try:
            self.conn.close()
        except:
            ...
