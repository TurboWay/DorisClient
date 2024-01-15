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

MetaSql = """
select *
from(
select table_schema as database_name
,table_name
,table_type
,if(table_type='VIEW', concat('show create view ',table_schema,'.',table_name), concat('show create table ',table_schema,'.',table_name)) as ddl_sql
,if(table_type='VIEW', '', concat('show partitions from ',table_schema,'.',table_name)) as partitions_sql
,if(table_type='VIEW', '', concat('show tablets from ',table_schema,'.',table_name)) as tablets_sql
from information_schema.tables
where table_type not in ('SYSTEM VIEW')
) s
where 1=1
order by 1,3,2
limit 100000000
"""

MetaSql_tablets = """
select database_name
,table_name
,PartitionId
,PartitionName
,concat('show tablets from ',database_name,'.',table_name,' partition ',PartitionName) as tablets_sql
from meta_partition
where 1=1 
order by 1,2
limit 100000000
"""


MetaDDL_Table = """
CREATE TABLE IF NOT EXISTS `meta_table` (
  `database_name` varchar(64) NULL COMMENT "databse name",
  `table_name` varchar(128) NULL COMMENT "table name",
  `table_type` varchar(64) NULL COMMENT "BASE TABLE, VIEW",
  `engine` varchar(64) NULL COMMENT "OLAP and others",
  `model` varchar(20) NULL COMMENT "Aggregate, Unique, Duplicate",
  `replication_num` int NULL COMMENT "replication_num",
  `bucket_num` int NULL COMMENT "bucket_num",  
  `properties` text NULL COMMENT "properties json",  
  `ddl` text NULL COMMENT "create table sql",
  `update_time` datetime NULL COMMENT "update_time"
) ENGINE=OLAP
COMMENT "meta of tables/views"
DISTRIBUTED BY HASH(`table_name`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 3",
"in_memory" = "false",
"storage_format" = "V2"
);
"""

MetaDDL_Tablet = """
CREATE TABLE IF NOT EXISTS `meta_tablet` (
  `database_name` varchar(64) NULL COMMENT "databse name",
  `table_name` varchar(128) NULL COMMENT "table name",
  `PartitionName` varchar(128) NULL,
  `PartitionId` bigint(20) NULL,
  `TabletId` bigint(20) NULL COMMENT "",
  `ReplicaId` bigint(20) NULL COMMENT "",
  `BackendId` bigint(20) NULL COMMENT "",
  `SchemaHash` bigint(20) NULL COMMENT "",
  `Version` bigint(20) NULL COMMENT "",
  `LstSuccessVersion` bigint(20) NULL COMMENT "",
  `LstFailedVersion` bigint(20) NULL COMMENT "",
  `LstFailedTime` datetime NULL COMMENT "",
  `DataSize` bigint(20) NULL COMMENT "",
  `RowCount` bigint(20) NULL COMMENT "",
  `State` varchar(64) NULL COMMENT "",
  `LstConsistencyCheckTime` datetime NULL COMMENT "",
  `CheckVersion` bigint(20) NULL COMMENT "",
  `VersionCount` bigint(20) NULL COMMENT "",
  `PathHash` bigint(20) NULL COMMENT "",
  `MetaUrl` varchar(500) NULL COMMENT "",
  `CompactionStatus` varchar(500) NULL COMMENT "",
  `update_time` datetime NULL COMMENT "update time"
) ENGINE=OLAP
DUPLICATE KEY(`database_name`)
COMMENT "meta of table tablets"
DISTRIBUTED BY HASH(`table_name`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 3",
"in_memory" = "false",
"storage_format" = "V2"
);
"""

MetaDDL_Partition = """
CREATE TABLE IF NOT EXISTS `meta_partition` (
  `database_name` varchar(64) NULL COMMENT "databse name",
  `table_name` varchar(128) NULL COMMENT "table name",
  `PartitionId` bigint(20) NULL COMMENT "",
  `PartitionName` varchar(128) NULL COMMENT "",
  `VisibleVersion` int(11) NULL COMMENT "",
  `VisibleVersionTime` datetime NULL COMMENT "",
  `State` varchar(64) NULL COMMENT "",
  `PartitionKey` varchar(64) NULL COMMENT "",
  `Range` varchar(500) NULL COMMENT "",
  `DistributionKey` varchar(2046) NULL COMMENT "",
  `Buckets` int(11) NULL COMMENT "",
  `ReplicationNum` int(11) NULL COMMENT "",
  `StorageMedium` varchar(64) NULL COMMENT "",
  `CooldownTime` datetime NULL COMMENT "",
  `LastConsistencyCheckTime` datetime NULL COMMENT "",
  `DataSize` varchar(64) NULL COMMENT "",
  `IsInMemory` varchar(8) NULL COMMENT "",
  `ReplicaAllocation` varchar(64) NULL COMMENT "",
  `update_time` datetime NULL COMMENT "update time"
) ENGINE=OLAP
COMMENT "meta of table partitions"
DISTRIBUTED BY HASH(`table_name`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 3",
"in_memory" = "false",
"storage_format" = "V2"
);
"""

MetaDDL_Size = """
CREATE TABLE IF NOT EXISTS `meta_size` (
  `database_name` varchar(64) NULL COMMENT "databse name",
  `table_name` varchar(128) NULL COMMENT "table name",
  `Size` varchar(128) NULL COMMENT "",
  `SizeByte` double NULL COMMENT "Size by Byte for order or analyze",
  `ReplicaCount` bigint NULL COMMENT "",
  `update_time` datetime NULL COMMENT "update time"
) ENGINE=OLAP
COMMENT "meta of table size"
DISTRIBUTED BY HASH(`table_name`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 3",
"in_memory" = "false",
"storage_format" = "V2"
);
"""
