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

package com.ververica.cdc.connectors.base.config;

import org.apache.flink.annotation.Internal;

import com.ververica.cdc.connectors.base.config.SourceConfig.Factory;
import com.ververica.cdc.connectors.base.options.JdbcSourceOptions;
import com.ververica.cdc.connectors.base.options.SourceOptions;
import com.ververica.cdc.connectors.base.options.StartupOptions;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/** A {@link Factory} to provide {@link SourceConfig} of JDBC data source. */
@Internal
public abstract class JdbcSourceConfigFactory implements Factory {

    private static final long serialVersionUID = 1L;

    protected int port;
    protected String hostname;
    protected String username;
    protected String password;
    protected List<String> databaseList;
    protected List<String> tableList;
    protected StartupOptions startupOptions = StartupOptions.initial();
    protected boolean includeSchemaChanges = false;
    protected double distributionFactorUpper =
            SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue();
    protected double distributionFactorLower =
            SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue();
    protected int splitSize = SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue();
    protected int splitMetaGroupSize = SourceOptions.CHUNK_META_GROUP_SIZE.defaultValue();
    protected int fetchSize = SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE.defaultValue();
    protected String serverTimeZone = JdbcSourceOptions.SERVER_TIME_ZONE.defaultValue();
    protected Duration connectTimeout = JdbcSourceOptions.CONNECT_TIMEOUT.defaultValue();
    protected int connectMaxRetries = JdbcSourceOptions.CONNECT_MAX_RETRIES.defaultValue();
    protected int connectionPoolSize = JdbcSourceOptions.CONNECTION_POOL_SIZE.defaultValue();
    protected Properties dbzProperties;

    /** Integer port number of the database server. */
    public JdbcSourceConfigFactory hostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    /** Integer port number of the database server. */
    public JdbcSourceConfigFactory port(int port) {
        this.port = port;
        return this;
    }

    /**
     * An optional list of regular expressions that match database names to be monitored; any
     * database name not included in the whitelist will be excluded from monitoring. By default all
     * databases will be monitored.
     */
    public JdbcSourceConfigFactory databaseList(String... databaseList) {
        this.databaseList = Arrays.asList(databaseList);
        return this;
    }

    /**
     * An optional list of regular expressions that match fully-qualified table identifiers for
     * tables to be monitored; any table not included in the list will be excluded from monitoring.
     * Each identifier is of t、he form databaseName.tableName. By default the connector will monitor
     * every non-system table in each monitored database.
     */
    public JdbcSourceConfigFactory tableList(String... tableList) {
        this.tableList = Arrays.asList(tableList);
        return this;
    }

    /** Name of the user to use when connecting to the database server. */
    public JdbcSourceConfigFactory username(String username) {
        this.username = username;
        return this;
    }

    /** Password to use when connecting to the database server. */
    public JdbcSourceConfigFactory password(String password) {
        this.password = password;
        return this;
    }

    /**
     * The session time zone in database server, e.g. "America/Los_Angeles". It controls how the
     * TIMESTAMP type in MYSQL converted to STRING. See more
     * https://debezium.io/documentation/reference/1.5/connectors/mysql.html#mysql-temporal-types
     */
    public JdbcSourceConfigFactory serverTimeZone(String timeZone) {
        this.serverTimeZone = timeZone;
        return this;
    }

    /**
     * The split size (number of rows) of table snapshot, captured tables are split into multiple
     * splits when read the snapshot of table.
     */
    public JdbcSourceConfigFactory splitSize(int splitSize) {
        this.splitSize = splitSize;
        return this;
    }

    /**
     * The group size of split meta, if the meta size exceeds the group size, the meta will be
     * divided into multiple groups.
     */
    public JdbcSourceConfigFactory splitMetaGroupSize(int splitMetaGroupSize) {
        this.splitMetaGroupSize = splitMetaGroupSize;
        return this;
    }

    /**
     * The upper bound of split key evenly distribution factor, the factor is used to determine
     * whether the table is evenly distribution or not.
     */
    public JdbcSourceConfigFactory distributionFactorUpper(double distributionFactorUpper) {
        this.distributionFactorUpper = distributionFactorUpper;
        return this;
    }

    /**
     * The lower bound of split key evenly distribution factor, the factor is used to determine
     * whether the table is evenly distribution or not.
     */
    public JdbcSourceConfigFactory distributionFactorLower(double distributionFactorLower) {
        this.distributionFactorLower = distributionFactorLower;
        return this;
    }

    /** The maximum fetch size for per poll when read table snapshot. */
    public JdbcSourceConfigFactory fetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }

    /**
     * The maximum time that the connector should wait after trying to connect to the MySQL database
     * server before timing out.
     */
    public JdbcSourceConfigFactory connectTimeout(Duration connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }

    /** The connection pool size. */
    public JdbcSourceConfigFactory connectionPoolSize(int connectionPoolSize) {
        this.connectionPoolSize = connectionPoolSize;
        return this;
    }

    /** The max retry times to get connection. */
    public JdbcSourceConfigFactory connectMaxRetries(int connectMaxRetries) {
        this.connectMaxRetries = connectMaxRetries;
        return this;
    }

    /** Whether the {@link SourceConfig} should output the schema changes or not. */
    public JdbcSourceConfigFactory includeSchemaChanges(boolean includeSchemaChanges) {
        this.includeSchemaChanges = includeSchemaChanges;
        return this;
    }

    /** The Debezium MySQL connector properties. For example, "snapshot.mode". */
    public JdbcSourceConfigFactory debeziumProperties(Properties properties) {
        this.dbzProperties = properties;
        return this;
    }

    /** Specifies the startup options. */
    public JdbcSourceConfigFactory startupOptions(StartupOptions startupOptions) {
        switch (startupOptions.startupMode) {
            case INITIAL:
            case LATEST_OFFSET:
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported startup mode: " + startupOptions.startupMode);
        }
        this.startupOptions = startupOptions;
        return this;
    }

    @Override
    public abstract JdbcSourceConfig create(int subtask);
}
