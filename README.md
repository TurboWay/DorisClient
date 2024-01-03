# DorisClient

python for apache-doris

# Install

```shell
pip install DorisClient
```

# Use

## Create Test Table

```sql
CREATE TABLE `streamload_test` (
  `id` int(11) NULL COMMENT "",
  `shop_code` varchar(64) NULL COMMENT "",
  `sale_amount` decimal(18, 2) NULL COMMENT ""
) ENGINE=OLAP
UNIQUE KEY(`id`)
COMMENT "test"
DISTRIBUTED BY HASH(`id`) BUCKETS 3
PROPERTIES (
"replication_allocation" = "tag.location.default: 3",
"in_memory" = "false",
"storage_format" = "V2"
);

-- If you want to enable sequence streamload, make sure Doris table enable sequence load first
-- ALTER TABLE streamload_test ENABLE FEATURE "SEQUENCE_LOAD" WITH PROPERTIES ("function_column.sequence_type" = "bigint");
```

## streamload

```python
from DorisClient import DorisSession, DorisLogger, Logger

# DorisLogger.setLevel('ERROR')  # default:INFO

doris_cfg = {
    'fe_servers': ['10.211.7.131:8030', '10.211.7.132:8030', '10.211.7.133:8030'],
    'database': 'testdb',
    'user': 'test',
    'passwd': '123456',
}
doris = DorisSession(**doris_cfg)

# append
data = [
    {'id': '1', 'shop_code': 'sdd1', 'sale_amount': '99'},
    {'id': '2', 'shop_code': 'sdd2', 'sale_amount': '5'},
    {'id': '3', 'shop_code': 'sdd3', 'sale_amount': '3'},
]
doris.streamload('streamload_test', data)

# delete
data = [
    {'id': '1'},
]
doris.streamload('streamload_test', data, merge_type='DELETE')

# merge
data = [
    {'id': '10', 'shop_code': 'sdd1', 'sale_amount': '99', 'delete_flag': 0},
    {'id': '2', 'shop_code': 'sdd2', 'sale_amount': '5', 'delete_flag': 1},
    {'id': '3', 'shop_code': 'sdd3', 'sale_amount': '3', 'delete_flag': 1},
]
doris.streamload('streamload_test', data, merge_type='MERGE', delete='delete_flag=1')

# Sequence append
data = [
    {'id': '1', 'shop_code': 'sdd1', 'sale_amount': '99', 'source_sequence': 11, },
    {'id': '1', 'shop_code': 'sdd2', 'sale_amount': '5', 'source_sequence': 2},
    {'id': '2', 'shop_code': 'sdd3', 'sale_amount': '3', 'source_sequence': 1},
]
doris.streamload('streamload_test', data, sequence_col='source_sequence')

# Sequence merge
data = [
    {'id': '1', 'shop_code': 'sdd1', 'sale_amount': '99', 'source_sequence': 100, 'delete_flag': 0},
    {'id': '1', 'shop_code': 'sdd2', 'sale_amount': '5', 'source_sequence': 120, 'delete_flag': 0},
    {'id': '2', 'shop_code': 'sdd3', 'sale_amount': '3', 'source_sequence': 100, 'delete_flag': 1},
]
doris.streamload('streamload_test', data, sequence_col='source_sequence', merge_type='MERGE',
                 delete='delete_flag=1')


# streamload default retry config:  max_retry=3, retry_diff_seconds=3
# if you don't want to retry, "_streamload" can help you
doris._streamload('streamload_test', data)

# if you want to changed retry config, follow code will work 
from DorisClient import DorisSession, Retry

max_retry = 5
retry_diff_seconds = 10


class MyDoris(DorisSession):

    @Retry(max_retry=max_retry, retry_diff_seconds=retry_diff_seconds)
    def streamload(self, table, dict_array, **kwargs):
        return self._streamload(table, dict_array, **kwargs)


doris = MyDoris(**doris_cfg)
doris.streamload('streamload_test', data)
```

## execute doris-sql

```python
from DorisClient import DorisSession

doris_cfg = {
    'fe_servers': ['10.211.7.131:8030', '10.211.7.132:8030', '10.211.7.133:8030'],
    'database': 'testdb',
    'user': 'test',
    'passwd': '123456',
}
doris = DorisSession(**doris_cfg)

sql = 'select * from streamload_test limit 1'

# fetch all the rows by sql, return dict array
rows = doris.read(sql)
print(rows)

# fetch all the rows by sql, return tuple array
rows = doris.read(sql, cursors=None)
print(rows)

# execute sql commit
doris.execute('truncate table streamload_test')
```

## collect meta

```python
from DorisClient import DorisMeta

doris_cfg = {
    'fe_servers': ['10.211.7.131:8030', '10.211.7.132:8030', '10.211.7.133:8030'],
    'database': 'testdb',
    'user': 'test',
    'passwd': '123456',
}
dm = DorisMeta(**doris_cfg)

# auto create table for collect doris meta
# 1. meta_table for saving all table meta
# 2. meta_tablet for saving all tablet meta
# 3. meta_partition for saving all partition meta
# 4. meta_size for saving all table size meta
dm.create_tables()

# collect table meta >> meta_table
dm.collect_table()

# collect partition meta >> meta_partition
dm.collect_partition()

# collect tablet meta >> meta_tablet 
# deploy collect_partition
dm.collect_tablet()

# collect table size meta >> meta_size
dm.collect_size()
```
