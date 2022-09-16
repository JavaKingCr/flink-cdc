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

import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.JdbcIncrementalSource;
import io.debezium.config.Configuration;

import java.util.Properties;

/** A basic Source configuration which is used by {@link JdbcIncrementalSource}. */
public abstract class BaseSourceConfig implements SourceConfig {

    private static final long serialVersionUID = 1L;

    protected final StartupOptions startupOptions;
    protected final int splitSize;
    protected final int splitMetaGroupSize;
    protected final double distributionFactorUpper;
    protected final double distributionFactorLower;
    protected final boolean includeSchemaChanges;

    // --------------------------------------------------------------------------------------------
    // Debezium Configurations
    // --------------------------------------------------------------------------------------------
    protected final Properties dbzProperties;
    protected transient Configuration dbzConfiguration;

    public BaseSourceConfig(
            StartupOptions startupOptions,
            int splitSize,
            int splitMetaGroupSize,
            double distributionFactorUpper,
            double distributionFactorLower,
            boolean includeSchemaChanges,
            Properties dbzProperties,
            Configuration dbzConfiguration) {
        this.startupOptions = startupOptions;
        this.splitSize = splitSize;
        this.splitMetaGroupSize = splitMetaGroupSize;
        this.distributionFactorUpper = distributionFactorUpper;
        this.distributionFactorLower = distributionFactorLower;
        this.includeSchemaChanges = includeSchemaChanges;
        this.dbzProperties = dbzProperties;
        this.dbzConfiguration = dbzConfiguration;
    }

    @Override
    public StartupOptions getStartupOptions() {
        return startupOptions;
    }

    @Override
    public int getSplitSize() {
        return splitSize;
    }

    @Override
    public int getSplitMetaGroupSize() {
        return splitMetaGroupSize;
    }

    @Override
    public double getDistributionFactorUpper() {
        return distributionFactorUpper;
    }

    @Override
    public double getDistributionFactorLower() {
        return distributionFactorLower;
    }

    @Override
    public boolean isIncludeSchemaChanges() {
        return includeSchemaChanges;
    }

    public Properties getDbzProperties() {
        return dbzProperties;
    }

    public Configuration getDbzConfiguration() {
        return Configuration.from(dbzProperties);
    }
}
