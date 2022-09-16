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

package com.ververica.cdc.connectors.mysql.source.events;

import org.apache.flink.api.connector.source.SourceEvent;

import com.ververica.cdc.connectors.mysql.source.enumerator.MySqlSourceEnumerator;
import com.ververica.cdc.connectors.mysql.source.reader.MySqlSourceReader;

/**
 * The {@link SourceEvent} that {@link MySqlSourceReader} sends to {@link MySqlSourceEnumerator} to
 * pull binlog meta data, i.e. sending {@link BinlogSplitMetaEvent}.
 */
public class BinlogSplitMetaRequestEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final String splitId;
    private final int requestMetaGroupId;

    public BinlogSplitMetaRequestEvent(String splitId, int requestMetaGroupId) {
        this.splitId = splitId;
        this.requestMetaGroupId = requestMetaGroupId;
    }

    public String getSplitId() {
        return splitId;
    }

    public int getRequestMetaGroupId() {
        return requestMetaGroupId;
    }
}
