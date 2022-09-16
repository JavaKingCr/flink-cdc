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

package com.ververica.cdc.connectors.mysql.debezium.reader;

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils;
import com.ververica.cdc.connectors.mysql.debezium.dispatcher.SignalEventDispatcher;
import com.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceTestBase;
import com.ververica.cdc.connectors.mysql.source.assigners.MySqlBinlogSplitAssigner;
import com.ververica.cdc.connectors.mysql.source.assigners.MySqlSnapshotSplitAssigner;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.mysql.source.split.SourceRecords;
import com.ververica.cdc.connectors.mysql.source.utils.RecordUtils;
import com.ververica.cdc.connectors.mysql.source.utils.TableDiscoveryUtils;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.connectors.mysql.testutils.RecordsFormatter;
import com.ververica.cdc.connectors.mysql.testutils.UniqueDatabase;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.getSnapshotSplitInfo;
import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.getStartingOffsetOfBinlogSplit;
import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.isHighWatermarkEvent;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Tests for {@link BinlogSplitReader}. */
public class BinlogSplitReaderTest extends MySqlSourceTestBase {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    private final UniqueDatabase customerDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "customer", "mysqluser", "mysqlpw");

    private BinaryLogClient binaryLogClient;
    private MySqlConnection mySqlConnection;

    @Test
    public void testReadSingleBinlogSplit() throws Exception {
        customerDatabase.createAndInitialize();
        MySqlSourceConfig sourceConfig = getConfig(new String[] {"customers_even_dist"});
        binaryLogClient = DebeziumUtils.createBinaryClient(sourceConfig.getDbzConfiguration());
        mySqlConnection = DebeziumUtils.createMySqlConnection(sourceConfig);
        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("address", DataTypes.STRING()),
                        DataTypes.FIELD("phone_number", DataTypes.STRING()));
        List<MySqlSnapshotSplit> splits =
                getMySqlSplits(new String[] {"customers_even_dist"}, sourceConfig);
        String[] expected =
                new String[] {
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "-D[102, user_2, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "-U[103, user_3, Shanghai, 123567891234]",
                    "+U[103, user_3, Hangzhou, 123567891234]",
                    "-U[103, user_3, Hangzhou, 123567891234]",
                    "+U[103, user_3, Shanghai, 123567891234]",
                    "+I[104, user_4, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]"
                };

        List<String> actual =
                readBinlogSplits(
                        splits,
                        dataType,
                        sourceConfig,
                        1,
                        expected.length,
                        splits.get(splits.size() - 1).getTableId());
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    @Test
    public void testReadAllBinlogSplitsForOneTable() throws Exception {
        customerDatabase.createAndInitialize();
        MySqlSourceConfig sourceConfig = getConfig(new String[] {"customers_even_dist"});
        binaryLogClient = DebeziumUtils.createBinaryClient(sourceConfig.getDbzConfiguration());
        mySqlConnection = DebeziumUtils.createMySqlConnection(sourceConfig);
        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("address", DataTypes.STRING()),
                        DataTypes.FIELD("phone_number", DataTypes.STRING()));
        List<MySqlSnapshotSplit> splits =
                getMySqlSplits(new String[] {"customers_even_dist"}, sourceConfig);

        String[] expected =
                new String[] {
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[104, user_4, Shanghai, 123567891234]",
                    "+I[105, user_5, Shanghai, 123567891234]",
                    "+I[106, user_6, Shanghai, 123567891234]",
                    "+I[107, user_7, Shanghai, 123567891234]",
                    "+I[108, user_8, Shanghai, 123567891234]",
                    "+I[109, user_9, Shanghai, 123567891234]",
                    "+I[110, user_10, Shanghai, 123567891234]",
                    "-U[103, user_3, Shanghai, 123567891234]",
                    "+U[103, user_3, Hangzhou, 123567891234]",
                    "-D[102, user_2, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "-U[103, user_3, Hangzhou, 123567891234]",
                    "+U[103, user_3, Shanghai, 123567891234]",
                    "+I[2001, user_22, Shanghai, 123567891234]",
                    "+I[2002, user_23, Shanghai, 123567891234]",
                    "+I[2003, user_24, Shanghai, 123567891234]"
                };
        List<String> actual =
                readBinlogSplits(
                        splits,
                        dataType,
                        sourceConfig,
                        splits.size(),
                        expected.length,
                        splits.get(splits.size() - 1).getTableId());
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    @Test
    public void testReadAllBinlogForTableWithSingleLine() throws Exception {
        customerDatabase.createAndInitialize();
        MySqlSourceConfig sourceConfig = getConfig(new String[] {"customer_card_single_line"});
        binaryLogClient = DebeziumUtils.createBinaryClient(sourceConfig.getDbzConfiguration());
        mySqlConnection = DebeziumUtils.createMySqlConnection(sourceConfig);

        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("card_no", DataTypes.BIGINT()),
                        DataTypes.FIELD("level", DataTypes.STRING()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("note", DataTypes.STRING()));
        List<MySqlSnapshotSplit> splits =
                getMySqlSplits(new String[] {"customer_card_single_line"}, sourceConfig);

        String[] expected =
                new String[] {
                    "+I[20000, LEVEL_1, user_1, user with level 1]",
                    "+I[20001, LEVEL_1, user_1, user with level 1]",
                    "+I[20001, LEVEL_2, user_2, user with level 2]",
                    "+I[20002, LEVEL_3, user_3, user with level 3]"
                };

        List<String> actual =
                readBinlogSplits(
                        splits,
                        dataType,
                        sourceConfig,
                        splits.size(),
                        expected.length,
                        splits.get(splits.size() - 1).getTableId());
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    @Test
    public void testReadAllBinlogSplitsForTables() throws Exception {
        customerDatabase.createAndInitialize();
        MySqlSourceConfig sourceConfig =
                getConfig(new String[] {"customer_card", "customer_card_single_line"});
        binaryLogClient = DebeziumUtils.createBinaryClient(sourceConfig.getDbzConfiguration());
        mySqlConnection = DebeziumUtils.createMySqlConnection(sourceConfig);
        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("card_no", DataTypes.BIGINT()),
                        DataTypes.FIELD("level", DataTypes.STRING()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("note", DataTypes.STRING()));
        List<MySqlSnapshotSplit> splits =
                getMySqlSplits(
                        new String[] {"customer_card", "customer_card_single_line"}, sourceConfig);
        String[] expected =
                new String[] {
                    "+I[20000, LEVEL_1, user_1, user with level 1]",
                    "+I[20001, LEVEL_1, user_1, user with level 1]",
                    "+I[20001, LEVEL_2, user_2, user with level 2]",
                    "+I[20002, LEVEL_3, user_3, user with level 3]",
                    "+I[20001, LEVEL_4, user_1, user with level 4]",
                    "+I[20002, LEVEL_4, user_2, user with level 4]",
                    "+I[20003, LEVEL_4, user_3, user with level 4]",
                    "+I[20004, LEVEL_1, user_4, user with level 4]",
                    "+I[20004, LEVEL_2, user_4, user with level 4]",
                    "+I[20004, LEVEL_3, user_4, user with level 4]",
                    "+I[20004, LEVEL_4, user_4, user with level 4]",
                    "+I[30006, LEVEL_3, user_5, user with level 3]",
                    "+I[30007, LEVEL_3, user_6, user with level 3]",
                    "+I[30008, LEVEL_3, user_7, user with level 3]",
                    "+I[30009, LEVEL_1, user_8, user with level 3]",
                    "+I[30009, LEVEL_2, user_8, user with level 3]",
                    "+I[30009, LEVEL_3, user_8, user with level 3]",
                    "+I[40001, LEVEL_2, user_9, user with level 2]",
                    "+I[40002, LEVEL_2, user_10, user with level 2]",
                    "+I[40003, LEVEL_2, user_11, user with level 2]",
                    "+I[50001, LEVEL_1, user_12, user with level 1]",
                    "+I[50002, LEVEL_1, user_13, user with level 1]",
                    "+I[50003, LEVEL_1, user_14, user with level 1]"
                };
        List<String> actual =
                readBinlogSplits(
                        splits,
                        dataType,
                        sourceConfig,
                        splits.size(),
                        expected.length,
                        // make the result deterministic
                        TableId.parse(
                                customerDatabase.getDatabaseName()
                                        + "."
                                        + "customer_card_single_line"));
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    @Test
    public void testReadBinlogFromLatestOffset() throws Exception {
        customerDatabase.createAndInitialize();
        MySqlSourceConfig sourceConfig =
                getConfig(StartupOptions.latest(), new String[] {"customers"});
        binaryLogClient = DebeziumUtils.createBinaryClient(sourceConfig.getDbzConfiguration());
        mySqlConnection = DebeziumUtils.createMySqlConnection(sourceConfig);

        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("address", DataTypes.STRING()),
                        DataTypes.FIELD("phone_number", DataTypes.STRING()));
        String[] expected =
                new String[] {
                    "-U[103, user_3, Shanghai, 123567891234]",
                    "+U[103, user_3, Hangzhou, 123567891234]",
                    "-D[102, user_2, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "-U[103, user_3, Hangzhou, 123567891234]",
                    "+U[103, user_3, Shanghai, 123567891234]",
                    "-U[1010, user_11, Shanghai, 123567891234]",
                    "+U[1010, Hangzhou, Shanghai, 123567891234]",
                    "+I[2001, user_22, Shanghai, 123567891234]",
                    "+I[2002, user_23, Shanghai, 123567891234]",
                    "+I[2003, user_24, Shanghai, 123567891234]"
                };
        List<String> actual =
                readBinlogSplitsFromLatestOffset(
                        createBinlogReader(sourceConfig), dataType, sourceConfig, expected.length);
        assertEqualsInOrder(Arrays.asList(expected), actual);
    }

    @Test
    public void testHeartbeatEvent() throws Exception {
        // Initialize database
        customerDatabase.createAndInitialize();

        // Set heartbeat interval to 500ms
        Duration heartbeatInterval = Duration.ofMillis(500);

        // Set keep alive interval to 500ms in order to let MySQl send heartbeat event to debezium
        // connector more frequently. Debezium MySQL connector only create heartbeat on receiving
        // events from MySQL.
        Properties dbzProps = new Properties();
        dbzProps.setProperty(
                MySqlConnectorConfig.KEEP_ALIVE_INTERVAL_MS.name(),
                String.valueOf(heartbeatInterval.toMillis()));

        // Create config and initializer client and connections
        MySqlSourceConfig sourceConfig =
                getConfigFactory(new String[] {"customers"})
                        .startupOptions(StartupOptions.latest())
                        .heartbeatInterval(heartbeatInterval)
                        .debeziumProperties(dbzProps)
                        .createConfig(0);
        binaryLogClient = DebeziumUtils.createBinaryClient(sourceConfig.getDbzConfiguration());
        mySqlConnection = DebeziumUtils.createMySqlConnection(sourceConfig);

        // Create binlog reader and submit split
        BinlogSplitReader binlogReader = createBinlogReader(sourceConfig);
        MySqlBinlogSplit binlogSplit = createBinlogSplitFromLatestOffset(sourceConfig);
        binlogReader.submitSplit(binlogSplit);

        // Make some change on the table
        makeCustomersBinlogEvents(
                mySqlConnection,
                binlogSplit.getTableSchemas().keySet().iterator().next().toString(),
                false);

        // Keep polling until we receive heartbeat. We don't validate offset of heartbeat here
        // because it's hard to know the expected offset value.
        List<SourceRecord> heartbeats = new ArrayList<>();
        CommonTestUtils.waitUtil(
                () -> {
                    heartbeats.addAll(
                            pollRecordsFromReader(binlogReader, RecordUtils::isHeartbeatEvent));
                    return !heartbeats.isEmpty();
                },
                DEFAULT_TIMEOUT,
                "Timeout waiting for heartbeat event");
    }

    private BinlogSplitReader createBinlogReader(MySqlSourceConfig sourceConfig) {
        return new BinlogSplitReader(
                new StatefulTaskContext(sourceConfig, binaryLogClient, mySqlConnection), 0);
    }

    private MySqlBinlogSplit createBinlogSplitFromLatestOffset(MySqlSourceConfig sourceConfig)
            throws Exception {
        MySqlBinlogSplitAssigner binlogSplitAssigner = new MySqlBinlogSplitAssigner(sourceConfig);
        binlogSplitAssigner.open();
        try (MySqlConnection jdbc = DebeziumUtils.createMySqlConnection(sourceConfig)) {
            Map<TableId, TableChanges.TableChange> tableSchemas =
                    TableDiscoveryUtils.discoverCapturedTableSchemas(sourceConfig, jdbc);
            return MySqlBinlogSplit.fillTableSchemas(
                    binlogSplitAssigner.getNext().get().asBinlogSplit(), tableSchemas);
        }
    }

    private List<SourceRecord> pollRecordsFromReader(
            BinlogSplitReader reader, Predicate<SourceRecord> filter) {
        List<SourceRecord> records = new ArrayList<>();
        Iterator<SourceRecords> recordIterator;
        try {
            recordIterator = reader.pollSplitRecords();
        } catch (InterruptedException e) {
            throw new RuntimeException("Polling action was interrupted", e);
        }
        if (recordIterator == null) {
            return records;
        }
        while (recordIterator.hasNext()) {
            Iterator<SourceRecord> iterator = recordIterator.next().iterator();
            while (iterator.hasNext()) {
                SourceRecord record = iterator.next();
                if (filter.test(record)) {
                    records.add(record);
                }
            }
        }
        LOG.debug("Records polled: {}", records);
        return records;
    }

    private List<String> readBinlogSplitsFromLatestOffset(
            BinlogSplitReader binlogReader,
            DataType dataType,
            MySqlSourceConfig sourceConfig,
            int expectedSize)
            throws Exception {

        // step-1: create binlog split
        MySqlBinlogSplit binlogSplit = createBinlogSplitFromLatestOffset(sourceConfig);

        // step-2: test read binlog split
        binlogReader.submitSplit(binlogSplit);

        // step-3: make some binlog events
        TableId tableId = binlogSplit.getTableSchemas().keySet().iterator().next();

        if (tableId.table().contains("customers")) {
            makeCustomersBinlogEvents(mySqlConnection, tableId.toString(), false);
        } else {
            makeCustomerCardsBinlogEvents(mySqlConnection, tableId.toString());
        }

        // step-4: fetched all produced binlog data and format them
        List<String> actual = new ArrayList<>();
        while (actual.size() < expectedSize) {
            actual.addAll(
                    formatResult(
                            pollRecordsFromReader(binlogReader, RecordUtils::isDataChangeRecord),
                            dataType));
        }
        return actual;
    }

    private List<String> readBinlogSplits(
            List<MySqlSnapshotSplit> sqlSplits,
            DataType dataType,
            MySqlSourceConfig sourceConfig,
            int scanSplitsNum,
            int expectedSize,
            TableId binlogChangeTableId)
            throws Exception {
        final StatefulTaskContext statefulTaskContext =
                new StatefulTaskContext(sourceConfig, binaryLogClient, mySqlConnection);
        final SnapshotSplitReader snapshotSplitReader =
                new SnapshotSplitReader(statefulTaskContext, 0);

        // step-1: read snapshot splits firstly
        List<SourceRecord> snapshotRecords = new ArrayList<>();
        for (int i = 0; i < scanSplitsNum; i++) {
            MySqlSplit sqlSplit = sqlSplits.get(i);
            if (snapshotSplitReader.isFinished()) {
                snapshotSplitReader.submitSplit(sqlSplit);
            }
            Iterator<SourceRecords> res;
            while ((res = snapshotSplitReader.pollSplitRecords()) != null) {
                while (res.hasNext()) {
                    Iterator<SourceRecord> iterator = res.next().iterator();
                    while (iterator.hasNext()) {
                        SourceRecord sourceRecord = iterator.next();
                        snapshotRecords.add(sourceRecord);
                    }
                }
            }
        }
        snapshotSplitReader.close();

        assertNotNull(snapshotSplitReader.getExecutorService());
        assertTrue(snapshotSplitReader.getExecutorService().isTerminated());

        // step-2: create binlog split according the finished snapshot splits
        List<FinishedSnapshotSplitInfo> finishedSplitsInfo =
                getFinishedSplitsInfo(sqlSplits, snapshotRecords);
        BinlogOffset startingOffset = getStartingOffsetOfBinlogSplit(finishedSplitsInfo);
        Map<TableId, TableChange> tableSchemas = new HashMap<>();
        for (MySqlSplit mySqlSplit : sqlSplits) {
            tableSchemas.putAll(mySqlSplit.getTableSchemas());
        }
        MySqlSplit binlogSplit =
                new MySqlBinlogSplit(
                        "binlog-split",
                        startingOffset,
                        BinlogOffset.NO_STOPPING_OFFSET,
                        finishedSplitsInfo,
                        tableSchemas,
                        finishedSplitsInfo.size());

        // step-3: test read binlog split
        BinlogSplitReader binlogReader = new BinlogSplitReader(statefulTaskContext, 0);
        binlogReader.submitSplit(binlogSplit);

        // step-4: make some binlog events
        if (binlogChangeTableId.table().contains("customers")) {
            makeCustomersBinlogEvents(
                    statefulTaskContext.getConnection(),
                    binlogChangeTableId.toString(),
                    scanSplitsNum == 1);
        } else {
            makeCustomerCardsBinlogEvents(
                    statefulTaskContext.getConnection(), binlogChangeTableId.toString());
        }

        // step-5: fetched all produced binlog data and format them
        // Put snapshot records into final collection first
        List<String> actual = new ArrayList<>(formatResult(snapshotRecords, dataType));
        while (actual.size() < expectedSize) {
            actual.addAll(
                    formatResult(
                            pollRecordsFromReader(binlogReader, RecordUtils::isDataChangeRecord),
                            dataType));
        }
        binlogReader.close();

        assertNotNull(snapshotSplitReader.getExecutorService());
        assertTrue(snapshotSplitReader.getExecutorService().isTerminated());

        return actual;
    }

    private void makeCustomersBinlogEvents(
            JdbcConnection connection, String tableId, boolean firstSplitOnly) throws SQLException {
        // make binlog events for the first split
        connection.setAutoCommit(false);
        connection.execute(
                "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 103",
                "DELETE FROM " + tableId + " where id = 102",
                "INSERT INTO " + tableId + " VALUES(102, 'user_2','Shanghai','123567891234')",
                "UPDATE " + tableId + " SET address = 'Shanghai' where id = 103");
        connection.commit();

        if (!firstSplitOnly) {
            // make binlog events for split-1
            connection.execute("UPDATE " + tableId + " SET name = 'Hangzhou' where id = 1010");
            connection.commit();

            // make binlog events for the last split
            connection.execute(
                    "INSERT INTO "
                            + tableId
                            + " VALUES(2001, 'user_22','Shanghai','123567891234')");
            connection.commit();

            // make schema change binlog events
            connection.execute(
                    "ALTER TABLE "
                            + tableId
                            + " ADD COLUMN email VARCHAR(128) DEFAULT 'user@flink.apache.org'");
            connection.commit();

            // make binlog events after schema changed
            connection.execute(
                    "INSERT INTO "
                            + tableId
                            + " VALUES(2002, 'user_23','Shanghai','123567891234', 'test1@gmail.com')");
            connection.commit();

            // make binlog again
            connection.execute(
                    "INSERT INTO "
                            + tableId
                            + " VALUES(2003, 'user_24','Shanghai','123567891234', 'test2@gmail.com')");
            connection.commit();
        }
    }

    private void makeCustomerCardsBinlogEvents(JdbcConnection connection, String tableId)
            throws SQLException {
        if (tableId.endsWith("customer_card_single_line")) {
            // make binlog events for the first split
            connection.execute(
                    "INSERT INTO "
                            + tableId
                            + " VALUES(20000, 'LEVEL_1', 'user_1', 'user with level 1')");
            connection.commit();

            // make binlog events for the last split
            connection.execute(
                    "INSERT INTO "
                            + tableId
                            + " VALUES(20001, 'LEVEL_2', 'user_2', 'user with level 2')",
                    "INSERT INTO "
                            + tableId
                            + " VALUES(20002, 'LEVEL_3', 'user_3', 'user with level 3')");

            connection.commit();
        } else {
            // make binlog events for the first split
            connection.execute(
                    "UPDATE " + tableId + " SET level = 'LEVEL_3' where user_id = 'user_1'",
                    "INSERT INTO "
                            + tableId
                            + " VALUES(20002, 'LEVEL_5', 'user_15', 'user with level 15'");
            connection.commit();

            // make binlog events for middle split
            connection.execute(
                    "INSERT INTO "
                            + tableId
                            + " VALUES(40000, 'LEVEL_1', 'user_16', 'user with level 1')",
                    "INSERT INTO "
                            + tableId
                            + " VALUES(40004, 'LEVEL_2', 'user_17', 'user with level 2')");
            connection.commit();

            // make binlog events for the last split
            connection.execute(
                    "INSERT INTO "
                            + tableId
                            + " VALUES(50004, 'LEVEL_1', 'user_18', 'user with level 1')",
                    "INSERT INTO "
                            + tableId
                            + " VALUES(50005, 'LEVEL_2', 'user_19', 'user with level 2')");
            connection.commit();
        }
    }

    private List<FinishedSnapshotSplitInfo> getFinishedSplitsInfo(
            List<MySqlSnapshotSplit> mySqlSplits, List<SourceRecord> records) {
        Map<String, MySqlSnapshotSplit> splitMap = new HashMap<>();
        mySqlSplits.forEach(r -> splitMap.put(r.splitId(), r));

        List<FinishedSnapshotSplitInfo> finishedSplitsInfo = new ArrayList<>();
        records.stream()
                .filter(event -> isHighWatermarkEvent(event))
                .forEach(
                        event -> {
                            Struct value = (Struct) event.value();
                            String splitId = value.getString(SignalEventDispatcher.SPLIT_ID_KEY);
                            MySqlSnapshotSplit mySqlSplit = splitMap.get(splitId);
                            finishedSplitsInfo.add(getSnapshotSplitInfo(mySqlSplit, event));
                        });
        return finishedSplitsInfo;
    }

    private List<String> formatResult(List<SourceRecord> records, DataType dataType) {
        final RecordsFormatter formatter = new RecordsFormatter(dataType);
        return formatter.format(records);
    }

    private List<MySqlSnapshotSplit> getMySqlSplits(
            String[] captureTables, MySqlSourceConfig sourceConfig) {
        List<String> captureTableIds =
                Arrays.stream(captureTables)
                        .map(tableName -> customerDatabase.getDatabaseName() + "." + tableName)
                        .collect(Collectors.toList());
        List<TableId> remainingTables =
                captureTableIds.stream().map(TableId::parse).collect(Collectors.toList());

        final MySqlSnapshotSplitAssigner assigner =
                new MySqlSnapshotSplitAssigner(
                        sourceConfig, DEFAULT_PARALLELISM, remainingTables, false);
        assigner.open();
        List<MySqlSnapshotSplit> mySqlSplits = new ArrayList<>();
        while (true) {
            Optional<MySqlSplit> mySqlSplit = assigner.getNext();
            if (mySqlSplit.isPresent()) {
                mySqlSplits.add(mySqlSplit.get().asSnapshotSplit());
            } else {
                break;
            }
        }
        assigner.close();
        return mySqlSplits;
    }

    private MySqlSourceConfig getConfig(StartupOptions startupOptions, String[] captureTables) {
        return getConfigFactory(captureTables).startupOptions(startupOptions).createConfig(0);
    }

    private MySqlSourceConfig getConfig(String[] captureTables) {
        return getConfigFactory(captureTables).createConfig(0);
    }

    private MySqlSourceConfigFactory getConfigFactory(String[] captureTables) {
        String[] captureTableIds =
                Arrays.stream(captureTables)
                        .map(tableName -> customerDatabase.getDatabaseName() + "." + tableName)
                        .toArray(String[]::new);

        return new MySqlSourceConfigFactory()
                .databaseList(customerDatabase.getDatabaseName())
                .tableList(captureTableIds)
                .hostname(MYSQL_CONTAINER.getHost())
                .port(MYSQL_CONTAINER.getDatabasePort())
                .username(customerDatabase.getUsername())
                .splitSize(4)
                .fetchSize(2)
                .password(customerDatabase.getPassword());
    }
}
