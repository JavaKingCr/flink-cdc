
flink 支持mysql 无主键数据同步

概述：修改源代码中 mysql 连接器  StartupOptions.initial 全量同步无主键表异常

问题：开源cdc 2.2.0 中 同步mysql 采用了全新的chunk 算法 https://flink-learning.org.cn/article/detail/3ebe9f20774991c4d5eeb75a141d9e1e,此算法全量同步表时包含两个步骤 全量读取chunk 以及合并读取chunk 时 binlog 的lowWatermark 和 highWatermark 的增量部分，在全量读取完成后会与增量部分的binlog合并，由于表没有主键 所以增量部分的binlog 无法替换全量中的数据（针对同一条数据而言），在源码中ChunkUtils.getChunkKeyColumn 

`  if (primaryKeys.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Incremental snapshot for tables requires primary key,"
                                    + " but table %s doesn't have primary key.",
                            table.id()));
            }
            `
            
 如果表不存在主键则直接抛出异常
 
 解决思路：将 binlog 的lowWatermark 和 highWatermark 的增量binlog 发出，用户自行保证消息的顺序性在下游幂等系统根据主键去重
 
 源码修改方法：
 找到 com.ververica.cdc.connectors.mysql.source.config 包下MySqlSourceConfig 增加
private final Boolean supportMysqlPrimaryKey; 属性
并在初始化时完成赋值：
<img width="633" alt="image" src="https://github.com/JavaKingCr/flink-cdc/assets/49787826/8707995c-2b9b-4610-ba48-a3b9ac258140">

 
 找到 com.ververica.cdc.connectors.mysql.debezium.reader 包下的SnapshotSplitReader中的pollSplitRecords方法
 添加：<img width="815" alt="image" src="https://github.com/JavaKingCr/flink-cdc/assets/49787826/d0f51ed6-3248-4e81-b967-7688cc254595">
`   Object snapshotRecords = null;
            if (!sourceConfig.getSupportMysqlPrimaryKey()) {
                snapshotRecords = new HashMap<Struct, SourceRecord>();
            } else {
                snapshotRecords = new LinkedList<SourceRecord>();
            }`

  在添加数据时做如下判断：
  <img width="1079" alt="image" src="https://github.com/JavaKingCr/flink-cdc/assets/49787826/935ab6e7-ac42-49c3-9142-703240fc86b3">

`   if (!reachBinlogStart) {
                        if (snapshotRecords.getClass() == HashMap.class) {
                            HashMap<Struct, SourceRecord> snapshotRecords1 = (HashMap<Struct, SourceRecord>) snapshotRecords;
                            snapshotRecords1.put((Struct) record.key(), record);
                        } else {
                            LinkedList<SourceRecord> snapshotRecords1 = (LinkedList<SourceRecord>) snapshotRecords;
                            snapshotRecords1.add(record);
                        }
//                        snapshotRecords.put((Struct) record.key(), record);
//                        snapshotRecords.add(record);
                    } else {
                        if (isRequiredBinlogRecord(record)) {
                            // upsert binlog events through the record key
                            upsertBinlog(snapshotRecords, record, sourceConfig.getSupportMysqlPrimaryKey());
                        }
                    }`
    修改：upsertBinlog  方法
    <img width="997" alt="image" src="https://github.com/JavaKingCr/flink-cdc/assets/49787826/178c1900-426c-4e5b-aa06-1fdd42811551">

   具体详细改动自行参考代码
 
 使用方法：
 将项目clone 本地，打maven 包 到本地，在自己项目中引用
 
 maven 打包命令：
`mvn install:install-file -Dfile=/Users/caorong/work/jar/flink-connector-mysql-cdc-2.3-SNAPSHOT.jar -DgroupId=com.pulan.flink -DartifactId=pulan-flink-cdc-mysql-2.12 -Dversion=2.2.1 -Dpackaging=jar`
 
`MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("")
                .port(3306)
                .databaseList("") // set captured database
                .tableList("") // set captured table
                .username("")
                .password("")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .supportNoMysqlPrimaryKey(true)
                .build();`

supportNoMysqlPrimaryKey(true) 开启无主键同步功能，不设置默认false
 
 
