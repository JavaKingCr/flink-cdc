/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.source;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowUtils;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExceptionUtils;

import com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils;
import com.ververica.cdc.connectors.mysql.table.MySqlDeserializationConverterFactory;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.connectors.mysql.testutils.UniqueDatabase;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.MetadataConverter;
import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.jdbc.JdbcConnection;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertTrue;

/** IT tests for {@link MySqlSource}. */
public class MySqlSourceITCase extends MySqlSourceTestBase {

    @Rule public final Timeout timeoutPerTest = Timeout.seconds(300);

    private final UniqueDatabase customDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "customer", "mysqluser", "mysqlpw");

    @Test
    public void testReadSingleTableWithSingleParallelism() throws Exception {
        testMySqlParallelSource(
                1, FailoverType.NONE, FailoverPhase.NEVER, new String[] {"customers"});
    }

    @Test
    public void testReadSingleTableWithMultipleParallelism() throws Exception {
        testMySqlParallelSource(
                4, FailoverType.NONE, FailoverPhase.NEVER, new String[] {"customers"});
    }

    @Test
    public void testReadMultipleTableWithSingleParallelism() throws Exception {
        testMySqlParallelSource(
                1,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                new String[] {"customers", "customers_1"});
    }

    @Test
    public void testReadMultipleTableWithMultipleParallelism() throws Exception {
        testMySqlParallelSource(
                4,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                new String[] {"customers", "customers_1"});
    }

    // Failover tests
    @Test
    public void testTaskManagerFailoverInSnapshotPhase() throws Exception {
        testMySqlParallelSource(
                FailoverType.TM, FailoverPhase.SNAPSHOT, new String[] {"customers", "customers_1"});
    }

    @Test
    public void testTaskManagerFailoverInBinlogPhase() throws Exception {
        testMySqlParallelSource(
                FailoverType.TM, FailoverPhase.BINLOG, new String[] {"customers", "customers_1"});
    }

    @Test
    public void testJobManagerFailoverInSnapshotPhase() throws Exception {
        testMySqlParallelSource(
                FailoverType.JM, FailoverPhase.SNAPSHOT, new String[] {"customers", "customers_1"});
    }

    @Test
    public void testJobManagerFailoverInBinlogPhase() throws Exception {
        testMySqlParallelSource(
                FailoverType.JM, FailoverPhase.BINLOG, new String[] {"customers", "customers_1"});
    }

    @Test
    public void testTaskManagerFailoverSingleParallelism() throws Exception {
        testMySqlParallelSource(
                1, FailoverType.TM, FailoverPhase.SNAPSHOT, new String[] {"customers"});
    }

    @Test
    public void testJobManagerFailoverSingleParallelism() throws Exception {
        testMySqlParallelSource(
                1, FailoverType.JM, FailoverPhase.SNAPSHOT, new String[] {"customers"});
    }

    @Test
    public void testNewlyAddedTableForExistsPipelineOnce() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testNewlyAddedTableForExistsPipelineOnceWithAheadBinlog() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testNewlyAddedTableForExistsPipelineTwice() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                false,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    public void testNewlyAddedTableForExistsPipelineTwiceWithAheadBinlog() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    public void testNewlyAddedTableForExistsPipelineSingleParallelism() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testNewlyAddedTableForExistsPipelineSingleParallelismWithAheadBinlog()
            throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testJobManagerFailoverForNewlyAddedTable() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.JM,
                FailoverPhase.SNAPSHOT,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testJobManagerFailoverForNewlyAddedTableWithAheadBinlog() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.JM,
                FailoverPhase.SNAPSHOT,
                true,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testTaskManagerFailoverForNewlyAddedTable() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                FailoverType.TM,
                FailoverPhase.BINLOG,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testTaskManagerFailoverForNewlyAddedTableWithAheadBinlog() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                FailoverType.TM,
                FailoverPhase.BINLOG,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testConsumingTableWithoutPrimaryKey() {
        try {
            testMySqlParallelSource(
                    1, FailoverType.NONE, FailoverPhase.NEVER, new String[] {"customers_no_pk"});
        } catch (Exception e) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    e,
                                    String.format(
                                            "Incremental snapshot for tables requires primary key, but table %s doesn't have primary key",
                                            customDatabase.getDatabaseName() + ".customers_no_pk"))
                            .isPresent());
        }
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testSnapshotSplitReadingFailCrossCheckpoints() throws Exception {
        customDatabase.createAndInitialize();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(5000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

        // The sleeping source will sleep awhile after send per record
        MySqlSource<RowData> sleepingSource = buildSleepingSource();
        DataStreamSource<RowData> source =
                env.fromSource(sleepingSource, WatermarkStrategy.noWatermarks(), "selfSource");

        String[] expectedSnapshotData =
                new String[] {
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[110, user_5, Shanghai, 123567891234]",
                    "+I[111, user_6, Shanghai, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                    "+I[1009, user_10, Shanghai, 123567891234]",
                    "+I[1010, user_11, Shanghai, 123567891234]",
                    "+I[1011, user_12, Shanghai, 123567891234]",
                    "+I[1012, user_13, Shanghai, 123567891234]",
                    "+I[1013, user_14, Shanghai, 123567891234]",
                    "+I[1014, user_15, Shanghai, 123567891234]",
                    "+I[1015, user_16, Shanghai, 123567891234]",
                    "+I[1016, user_17, Shanghai, 123567891234]",
                    "+I[1017, user_18, Shanghai, 123567891234]",
                    "+I[1018, user_19, Shanghai, 123567891234]",
                    "+I[1019, user_20, Shanghai, 123567891234]",
                    "+I[2000, user_21, Shanghai, 123567891234]"
                };
        TypeSerializer<RowData> serializer =
                source.getTransformation().getOutputType().createSerializer(env.getConfig());
        String accumulatorName = "dataStreamCollect_" + UUID.randomUUID();
        CollectSinkOperatorFactory<RowData> factory =
                new CollectSinkOperatorFactory(serializer, accumulatorName);
        CollectSinkOperator<RowData> operator = (CollectSinkOperator) factory.getOperator();
        CollectResultIterator<RowData> iterator =
                new CollectResultIterator(
                        operator.getOperatorIdFuture(),
                        serializer,
                        accumulatorName,
                        env.getCheckpointConfig());
        CollectStreamSink<RowData> sink = new CollectStreamSink(source, factory);
        sink.name("Data stream collect sink");
        env.addOperator(sink.getTransformation());
        JobClient jobClient = env.executeAsync("snapshotSplitTest");
        iterator.setJobClient(jobClient);
        JobID jobId = jobClient.getJobID();

        // Trigger failover once some snapshot records has been sent by sleeping source
        if (iterator.hasNext()) {
            triggerFailover(
                    FailoverType.JM,
                    jobId,
                    miniClusterResource.getMiniCluster(),
                    () -> sleepMs(100));
        }

        // Check all snapshot records are sent with exactly-once semantics
        assertEqualsInAnyOrder(
                Arrays.asList(expectedSnapshotData),
                fetchRowData(iterator, expectedSnapshotData.length));
        jobClient.cancel().get();
    }

    private MySqlSource<RowData> buildSleepingSource() {
        ResolvedSchema physicalSchema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("id", DataTypes.BIGINT().notNull()),
                                Column.physical("name", DataTypes.STRING()),
                                Column.physical("address", DataTypes.STRING()),
                                Column.physical("phone_number", DataTypes.STRING())),
                        new ArrayList<>(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));
        RowType physicalDataType =
                (RowType) physicalSchema.toPhysicalRowDataType().getLogicalType();
        MetadataConverter[] metadataConverters = new MetadataConverter[0];
        final TypeInformation<RowData> typeInfo = InternalTypeInfo.of(physicalDataType);

        SleepingRowDataDebeziumDeserializeSchema deserializer =
                new SleepingRowDataDebeziumDeserializeSchema(
                        RowDataDebeziumDeserializeSchema.newBuilder()
                                .setPhysicalRowType(physicalDataType)
                                .setMetadataConverters(metadataConverters)
                                .setResultTypeInfo(typeInfo)
                                .setServerTimeZone(ZoneId.of("UTC"))
                                .setUserDefinedConverterFactory(
                                        MySqlDeserializationConverterFactory.instance())
                                .build(),
                        1000L);
        return MySqlSource.<RowData>builder()
                .hostname(MYSQL_CONTAINER.getHost())
                .port(MYSQL_CONTAINER.getDatabasePort())
                .databaseList(customDatabase.getDatabaseName())
                .tableList(customDatabase.getDatabaseName() + ".customers")
                .username(customDatabase.getUsername())
                .password(customDatabase.getPassword())
                .serverTimeZone("UTC")
                .serverId(getServerId())
                .splitSize(8096)
                .splitMetaGroupSize(1000)
                .distributionFactorUpper(1000.0d)
                .distributionFactorLower(0.05d)
                .fetchSize(1024)
                .connectTimeout(Duration.ofSeconds(30))
                .connectMaxRetries(3)
                .connectionPoolSize(20)
                .debeziumProperties(new Properties())
                .startupOptions(StartupOptions.initial())
                .deserializer(deserializer)
                .scanNewlyAddedTableEnabled(false)
                .jdbcProperties(new Properties())
                .heartbeatInterval(Duration.ofSeconds(30))
                .build();
    }

    private void testMySqlParallelSource(
            FailoverType failoverType, FailoverPhase failoverPhase, String[] captureCustomerTables)
            throws Exception {
        testMySqlParallelSource(
                DEFAULT_PARALLELISM, failoverType, failoverPhase, captureCustomerTables);
    }

    private void testMySqlParallelSource(
            int parallelism,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String[] captureCustomerTables)
            throws Exception {
        customDatabase.createAndInitialize();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
        String sourceDDL =
                format(
                        "CREATE TABLE customers ("
                                + " id BIGINT NOT NULL,"
                                + " name STRING,"
                                + " address STRING,"
                                + " phone_number STRING,"
                                + " primary key (id) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'scan.incremental.snapshot.enabled' = 'true',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.size' = '100',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'server-id' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        customDatabase.getUsername(),
                        customDatabase.getPassword(),
                        customDatabase.getDatabaseName(),
                        getTableNameRegex(captureCustomerTables),
                        getServerId());
        // first step: check the snapshot data
        String[] snapshotForSingleTable =
                new String[] {
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[110, user_5, Shanghai, 123567891234]",
                    "+I[111, user_6, Shanghai, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                    "+I[1009, user_10, Shanghai, 123567891234]",
                    "+I[1010, user_11, Shanghai, 123567891234]",
                    "+I[1011, user_12, Shanghai, 123567891234]",
                    "+I[1012, user_13, Shanghai, 123567891234]",
                    "+I[1013, user_14, Shanghai, 123567891234]",
                    "+I[1014, user_15, Shanghai, 123567891234]",
                    "+I[1015, user_16, Shanghai, 123567891234]",
                    "+I[1016, user_17, Shanghai, 123567891234]",
                    "+I[1017, user_18, Shanghai, 123567891234]",
                    "+I[1018, user_19, Shanghai, 123567891234]",
                    "+I[1019, user_20, Shanghai, 123567891234]",
                    "+I[2000, user_21, Shanghai, 123567891234]"
                };
        tEnv.executeSql(sourceDDL);
        TableResult tableResult = tEnv.executeSql("select * from customers");
        CloseableIterator<Row> iterator = tableResult.collect();
        JobID jobId = tableResult.getJobClient().get().getJobID();
        List<String> expectedSnapshotData = new ArrayList<>();
        for (int i = 0; i < captureCustomerTables.length; i++) {
            expectedSnapshotData.addAll(Arrays.asList(snapshotForSingleTable));
        }

        // trigger failover after some snapshot splits read finished
        if (failoverPhase == FailoverPhase.SNAPSHOT && iterator.hasNext()) {
            triggerFailover(
                    failoverType, jobId, miniClusterResource.getMiniCluster(), () -> sleepMs(100));
        }

        assertEqualsInAnyOrder(
                expectedSnapshotData, fetchRows(iterator, expectedSnapshotData.size()));

        // second step: check the binlog data
        for (String tableId : captureCustomerTables) {
            makeFirstPartBinlogEvents(
                    getConnection(), customDatabase.getDatabaseName() + '.' + tableId);
        }
        if (failoverPhase == FailoverPhase.BINLOG) {
            triggerFailover(
                    failoverType, jobId, miniClusterResource.getMiniCluster(), () -> sleepMs(200));
        }
        for (String tableId : captureCustomerTables) {
            makeSecondPartBinlogEvents(
                    getConnection(), customDatabase.getDatabaseName() + '.' + tableId);
        }

        String[] binlogForSingleTable =
                new String[] {
                    "-U[103, user_3, Shanghai, 123567891234]",
                    "+U[103, user_3, Hangzhou, 123567891234]",
                    "-D[102, user_2, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "-U[103, user_3, Hangzhou, 123567891234]",
                    "+U[103, user_3, Shanghai, 123567891234]",
                    "-U[1010, user_11, Shanghai, 123567891234]",
                    "+U[1010, user_11, Hangzhou, 123567891234]",
                    "+I[2001, user_22, Shanghai, 123567891234]",
                    "+I[2002, user_23, Shanghai, 123567891234]",
                    "+I[2003, user_24, Shanghai, 123567891234]"
                };
        List<String> expectedBinlogData = new ArrayList<>();
        for (int i = 0; i < captureCustomerTables.length; i++) {
            expectedBinlogData.addAll(Arrays.asList(binlogForSingleTable));
        }
        assertEqualsInAnyOrder(expectedBinlogData, fetchRows(iterator, expectedBinlogData.size()));
        tableResult.getJobClient().get().cancel().get();
    }

    private static List<String> convertRowDataToRowString(List<RowData> rows) {
        LinkedHashMap<String, Integer> map = new LinkedHashMap<>();
        map.put("id", 0);
        map.put("name", 1);
        map.put("address", 2);
        map.put("phone_number", 3);
        return rows.stream()
                .map(
                        row ->
                                RowUtils.createRowWithNamedPositions(
                                                row.getRowKind(),
                                                new Object[] {
                                                    row.getLong(0),
                                                    row.getString(1),
                                                    row.getString(2),
                                                    row.getString(3)
                                                },
                                                map)
                                        .toString())
                .collect(Collectors.toList());
    }

    private void testNewlyAddedTableOneByOne(
            int parallelism,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            boolean makeBinlogBeforeCapture,
            String... captureAddressTables)
            throws Exception {

        // step 1: create mysql tables with initial data
        TestValuesTableFactory.clearAllData();
        customDatabase.createAndInitialize();
        initialAddressTables(getConnection(), captureAddressTables);

        final TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        final String savepointDirectory = temporaryFolder.newFolder().toURI().toString();

        // test newly added table one by one
        String finishedSavePointPath = null;
        List<String> fetchedDataList = new ArrayList<>();
        for (int round = 0; round < captureAddressTables.length; round++) {
            String[] captureTablesThisRound =
                    Arrays.asList(captureAddressTables)
                            .subList(0, round + 1)
                            .toArray(new String[0]);
            String newlyAddedTable = captureAddressTables[round];
            if (makeBinlogBeforeCapture) {
                makeBinlogBeforeCaptureForAddressTable(getConnection(), newlyAddedTable);
            }
            StreamExecutionEnvironment env =
                    getStreamExecutionEnvironment(finishedSavePointPath, parallelism);
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

            String createTableStatement = getCreateTableStatement(captureTablesThisRound);
            tEnv.executeSql(createTableStatement);
            tEnv.executeSql(
                    "CREATE TABLE sink ("
                            + " table_name STRING,"
                            + " id BIGINT,"
                            + " country STRING,"
                            + " city STRING,"
                            + " detail_address STRING,"
                            + " primary key (id) not enforced"
                            + ") WITH ("
                            + " 'connector' = 'values',"
                            + " 'sink-insert-only' = 'false'"
                            + ")");
            TableResult tableResult = tEnv.executeSql("insert into sink select * from address");
            JobClient jobClient = tableResult.getJobClient().get();

            // step 2: assert fetched snapshot data in this round
            String cityName = newlyAddedTable.split("_")[1];
            List<String> expectedSnapshotDataThisRound =
                    Arrays.asList(
                            format(
                                    "+I[%s, 416874195632735147, China, %s, %s West Town address 1]",
                                    newlyAddedTable, cityName, cityName),
                            format(
                                    "+I[%s, 416927583791428523, China, %s, %s West Town address 2]",
                                    newlyAddedTable, cityName, cityName),
                            format(
                                    "+I[%s, 417022095255614379, China, %s, %s West Town address 3]",
                                    newlyAddedTable, cityName, cityName));
            if (makeBinlogBeforeCapture) {
                expectedSnapshotDataThisRound =
                        Arrays.asList(
                                format(
                                        "+I[%s, 416874195632735147, China, %s, %s West Town address 1]",
                                        newlyAddedTable, cityName, cityName),
                                format(
                                        "+I[%s, 416927583791428523, China, %s, %s West Town address 2]",
                                        newlyAddedTable, cityName, cityName),
                                format(
                                        "+I[%s, 417022095255614379, China, %s, %s West Town address 3]",
                                        newlyAddedTable, cityName, cityName),
                                format(
                                        "+I[%s, 417022095255614381, China, %s, %s West Town address 5]",
                                        newlyAddedTable, cityName, cityName));
            }

            // trigger failover after some snapshot data read finished
            if (failoverPhase == FailoverPhase.SNAPSHOT) {
                triggerFailover(
                        failoverType,
                        jobClient.getJobID(),
                        miniClusterResource.getMiniCluster(),
                        () -> sleepMs(100));
            }
            fetchedDataList.addAll(expectedSnapshotDataThisRound);
            waitForSinkSize("sink", fetchedDataList.size());
            assertEqualsInAnyOrder(fetchedDataList, TestValuesTableFactory.getRawResults("sink"));

            // step 3: make some binlog data for this round
            makeFirstPartBinlogForAddressTable(getConnection(), newlyAddedTable);
            if (failoverPhase == FailoverPhase.BINLOG) {
                triggerFailover(
                        failoverType,
                        jobClient.getJobID(),
                        miniClusterResource.getMiniCluster(),
                        () -> sleepMs(100));
            }
            makeSecondPartBinlogForAddressTable(getConnection(), newlyAddedTable);

            // step 4: assert fetched binlog data in this round
            List<String> expectedBinlogDataThisRound =
                    Arrays.asList(
                            format(
                                    "+U[%s, 416874195632735147, CHINA, %s, %s West Town address 1]",
                                    newlyAddedTable, cityName, cityName),
                            format(
                                    "+I[%s, 417022095255614380, China, %s, %s West Town address 4]",
                                    newlyAddedTable, cityName, cityName));

            // step 5: assert fetched binlog data in this round
            fetchedDataList.addAll(expectedBinlogDataThisRound);
            waitForSinkSize("sink", fetchedDataList.size());
            assertEqualsInAnyOrder(fetchedDataList, TestValuesTableFactory.getRawResults("sink"));

            // step 6: trigger savepoint
            if (round != captureAddressTables.length - 1) {
                finishedSavePointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
            }
            jobClient.cancel().get();
        }
    }

    private String getCreateTableStatement(String... captureTableNames) {
        return format(
                "CREATE TABLE address ("
                        + " table_name STRING METADATA VIRTUAL,"
                        + " id BIGINT NOT NULL,"
                        + " country STRING,"
                        + " city STRING,"
                        + " detail_address STRING,"
                        + " primary key (id) not enforced"
                        + ") WITH ("
                        + " 'connector' = 'mysql-cdc',"
                        + " 'scan.incremental.snapshot.enabled' = 'true',"
                        + " 'hostname' = '%s',"
                        + " 'port' = '%s',"
                        + " 'username' = '%s',"
                        + " 'password' = '%s',"
                        + " 'database-name' = '%s',"
                        + " 'table-name' = '%s',"
                        + " 'scan.incremental.snapshot.chunk.size' = '2',"
                        + " 'server-time-zone' = 'UTC',"
                        + " 'server-id' = '%s',"
                        + " 'scan.newly-added-table.enabled' = 'true'"
                        + ")",
                MYSQL_CONTAINER.getHost(),
                MYSQL_CONTAINER.getDatabasePort(),
                customDatabase.getUsername(),
                customDatabase.getPassword(),
                customDatabase.getDatabaseName(),
                getTableNameRegex(captureTableNames),
                getServerId());
    }

    private StreamExecutionEnvironment getStreamExecutionEnvironment(
            String finishedSavePointPath, int parallelism) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (finishedSavePointPath != null) {
            // restore from savepoint
            // hack for test to visit protected TestStreamEnvironment#getConfiguration() method
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            Class<?> clazz =
                    classLoader.loadClass(
                            "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment");
            Field field = clazz.getDeclaredField("configuration");
            field.setAccessible(true);
            Configuration configuration = (Configuration) field.get(env);
            configuration.setString(SavepointConfigOptions.SAVEPOINT_PATH, finishedSavePointPath);
        }
        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 100L));
        return env;
    }

    private String triggerSavepointWithRetry(JobClient jobClient, String savepointDirectory)
            throws ExecutionException, InterruptedException {
        int retryTimes = 0;
        // retry 600 times, it takes 100 milliseconds per time, at most retry 1 minute
        while (retryTimes < 600) {
            try {
                return jobClient.triggerSavepoint(savepointDirectory).get();
            } catch (Exception e) {
                Optional<CheckpointException> exception =
                        ExceptionUtils.findThrowable(e, CheckpointException.class);
                if (exception.isPresent()
                        && exception.get().getMessage().contains("Checkpoint triggering task")) {
                    Thread.sleep(100);
                    retryTimes++;
                } else {
                    throw e;
                }
            }
        }
        return null;
    }

    private static List<String> fetchRows(Iterator<Row> iter, int size) {
        List<String> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            Row row = iter.next();
            rows.add(row.toString());
            size--;
        }
        return rows;
    }

    private static List<String> fetchRowData(Iterator<RowData> iter, int size) {
        List<RowData> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            RowData row = iter.next();
            rows.add(row);
            size--;
        }
        return convertRowDataToRowString(rows);
    }

    private String getTableNameRegex(String[] captureCustomerTables) {
        checkState(captureCustomerTables.length > 0);
        if (captureCustomerTables.length == 1) {
            return captureCustomerTables[0];
        } else {
            // pattern that matches multiple tables
            return format("(%s)", StringUtils.join(captureCustomerTables, "|"));
        }
    }

    private String getServerId() {
        final Random random = new Random();
        int serverId = random.nextInt(100) + 5400;
        return serverId + "-" + (serverId + DEFAULT_PARALLELISM);
    }

    private void sleepMs(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
        }
    }

    private void makeFirstPartBinlogEvents(JdbcConnection connection, String tableId)
            throws SQLException {
        try {
            connection.setAutoCommit(false);

            // make binlog events for the first split
            connection.execute(
                    "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 103",
                    "DELETE FROM " + tableId + " where id = 102",
                    "INSERT INTO " + tableId + " VALUES(102, 'user_2','Shanghai','123567891234')",
                    "UPDATE " + tableId + " SET address = 'Shanghai' where id = 103");
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private void makeSecondPartBinlogEvents(JdbcConnection connection, String tableId)
            throws SQLException {
        try {
            connection.setAutoCommit(false);

            // make binlog events for split-1
            connection.execute("UPDATE " + tableId + " SET address = 'Hangzhou' where id = 1010");
            connection.commit();

            // make binlog events for the last split
            connection.execute(
                    "INSERT INTO "
                            + tableId
                            + " VALUES(2001, 'user_22','Shanghai','123567891234'),"
                            + " (2002, 'user_23','Shanghai','123567891234'),"
                            + "(2003, 'user_24','Shanghai','123567891234')");
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private void initialAddressTables(JdbcConnection connection, String[] addressTables)
            throws SQLException {
        try {
            connection.setAutoCommit(false);
            for (String tableName : addressTables) {
                // make initial data for given table
                String tableId = customDatabase.getDatabaseName() + "." + tableName;
                String cityName = tableName.split("_")[1];
                connection.execute(
                        "CREATE TABLE "
                                + tableId
                                + "("
                                + "  id BIGINT UNSIGNED NOT NULL PRIMARY KEY,"
                                + "  country VARCHAR(255) NOT NULL,"
                                + "  city VARCHAR(255) NOT NULL,"
                                + "  detail_address VARCHAR(1024)"
                                + ");");
                connection.execute(
                        format(
                                "INSERT INTO  %s "
                                        + "VALUES (416874195632735147, 'China', '%s', '%s West Town address 1'),"
                                        + "       (416927583791428523, 'China', '%s', '%s West Town address 2'),"
                                        + "       (417022095255614379, 'China', '%s', '%s West Town address 3');",
                                tableId, cityName, cityName, cityName, cityName, cityName,
                                cityName));
            }
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private void makeFirstPartBinlogForAddressTable(JdbcConnection connection, String tableName)
            throws SQLException {
        try {
            connection.setAutoCommit(false);
            // make binlog events for the first split
            String tableId = customDatabase.getDatabaseName() + "." + tableName;
            String cityName = tableName.split("_")[1];
            connection.execute(
                    format(
                            "UPDATE %s SET COUNTRY = 'CHINA' where id = 416874195632735147",
                            tableId));
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private void makeSecondPartBinlogForAddressTable(JdbcConnection connection, String tableName)
            throws SQLException {
        try {
            connection.setAutoCommit(false);
            // make binlog events for the second split
            String tableId = customDatabase.getDatabaseName() + "." + tableName;
            String cityName = tableName.split("_")[1];
            connection.execute(
                    format(
                            "INSERT INTO %s VALUES(417022095255614380, 'China','%s','%s West Town address 4')",
                            tableId, cityName, cityName));
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private void makeBinlogBeforeCaptureForAddressTable(JdbcConnection connection, String tableName)
            throws SQLException {
        try {
            connection.setAutoCommit(false);
            // make binlog before the capture of the table
            String tableId = customDatabase.getDatabaseName() + "." + tableName;
            String cityName = tableName.split("_")[1];
            connection.execute(
                    format(
                            "INSERT INTO %s VALUES(417022095255614381, 'China','%s','%s West Town address 5')",
                            tableId, cityName, cityName));
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private MySqlConnection getConnection() {
        Map<String, String> properties = new HashMap<>();
        properties.put("database.hostname", MYSQL_CONTAINER.getHost());
        properties.put("database.port", String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        properties.put("database.user", customDatabase.getUsername());
        properties.put("database.password", customDatabase.getPassword());
        properties.put("database.serverTimezone", ZoneId.of("UTC").toString());
        io.debezium.config.Configuration configuration =
                io.debezium.config.Configuration.from(properties);
        return DebeziumUtils.createMySqlConnection(configuration, new Properties());
    }

    // ------------------------------------------------------------------------
    //  test utilities
    // ------------------------------------------------------------------------

    /** The type of failover. */
    private enum FailoverType {
        TM,
        JM,
        NONE
    }

    /** The phase of failover. */
    private enum FailoverPhase {
        SNAPSHOT,
        BINLOG,
        NEVER
    }

    private static void triggerFailover(
            FailoverType type, JobID jobId, MiniCluster miniCluster, Runnable afterFailAction)
            throws Exception {
        switch (type) {
            case TM:
                restartTaskManager(miniCluster, afterFailAction);
                break;
            case JM:
                triggerJobManagerFailover(jobId, miniCluster, afterFailAction);
                break;
            case NONE:
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
    }

    private static void triggerJobManagerFailover(
            JobID jobId, MiniCluster miniCluster, Runnable afterFailAction) throws Exception {
        final HaLeadershipControl haLeadershipControl = miniCluster.getHaLeadershipControl().get();
        haLeadershipControl.revokeJobMasterLeadership(jobId).get();
        afterFailAction.run();
        haLeadershipControl.grantJobMasterLeadership(jobId).get();
    }

    private static void restartTaskManager(MiniCluster miniCluster, Runnable afterFailAction)
            throws Exception {
        miniCluster.terminateTaskManager(0).get();
        afterFailAction.run();
        miniCluster.startTaskManager();
    }

    private static void waitForSinkSize(String sinkName, int expectedSize)
            throws InterruptedException {
        while (sinkSize(sinkName) < expectedSize) {
            Thread.sleep(100);
        }
    }

    private static int sinkSize(String sinkName) {
        synchronized (TestValuesTableFactory.class) {
            try {
                return TestValuesTableFactory.getRawResults(sinkName).size();
            } catch (IllegalArgumentException e) {
                // job is not started yet
                return 0;
            }
        }
    }

    /**
     * A {@link DebeziumDeserializationSchema} implementation which sleep given milliseconds after
     * deserialize per record, this class is designed for test.
     */
    static class SleepingRowDataDebeziumDeserializeSchema
            implements DebeziumDeserializationSchema<RowData> {

        private static final long serialVersionUID = 1L;

        private final RowDataDebeziumDeserializeSchema deserializeSchema;
        private final long sleepMs;

        public SleepingRowDataDebeziumDeserializeSchema(
                RowDataDebeziumDeserializeSchema deserializeSchema, long sleepMs) {
            this.deserializeSchema = deserializeSchema;
            this.sleepMs = sleepMs;
        }

        @Override
        public void deserialize(SourceRecord record, Collector<RowData> out) throws Exception {
            deserializeSchema.deserialize(record, out);
            Thread.sleep(sleepMs);
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            return deserializeSchema.getProducedType();
        }
    }
}
