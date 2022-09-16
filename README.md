概述：修改源代码中 mysql 连接器  StartupOptions.initial 全量同步无主键表异常

问题：开源cdc 2.2.0 中 同步mysql 采用了全新的chunk 算法 https://flink-learning.org.cn/article/detail/3ebe9f20774991c4d5eeb75a141d9e1e,此算法全量同步表时包含两个步骤 全量读取chunk 以及合并读取chunk 时 binlog 的lowWatermark 和 highWatermark 的增量部分，在全量读取完成后会与增量部分的binlog合并，由于表没有主键 所以增量部分的binlog 无法替换全量中的数据（针对同一条数据而言），在源码中ChunkUtils.getChunkKeyColumn 

  if (primaryKeys.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Incremental snapshot for tables requires primary key,"
                                    + " but table %s doesn't have primary key.",
                            table.id()));
            }
            
            
 如果表不存在主键则直接抛出异常
 
 解决思路：将 binlog 的lowWatermark 和 highWatermark 的增量binlog 发出，在下游去重。
 
 
 
 使用方法：
 将项目clone 本地，打maven 包 到本地，在自己项目中引用
      MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("")
                .port(3306)
                .databaseList("") // set captured database
                .tableList("") // set captured table
                .username("")
                .password("")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .supportNoMysqlPrimaryKey(true)
                .build();

supportNoMysqlPrimaryKey(true) 开启无主键同步功能，不设置默认false
 
 
