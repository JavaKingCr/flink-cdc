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

package com.ververica.cdc.connectors.base.source.meta.split;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;

import javax.annotation.Nullable;

import java.util.Map;

/** The state of split to describe the binlog of table(s). */
public class StreamSplitState extends SourceSplitState {

    @Nullable private Offset startingOffset;
    @Nullable private Offset endingOffset;
    private final Map<TableId, TableChange> tableSchemas;

    public StreamSplitState(StreamSplit split) {
        super(split);
        this.startingOffset = split.getStartingOffset();
        this.endingOffset = split.getEndingOffset();
        this.tableSchemas = split.getTableSchemas();
    }

    @Nullable
    public Offset getStartingOffset() {
        return startingOffset;
    }

    public void setStartingOffset(@Nullable Offset startingOffset) {
        this.startingOffset = startingOffset;
    }

    @Nullable
    public Offset getEndingOffset() {
        return endingOffset;
    }

    public void setEndingOffset(@Nullable Offset endingOffset) {
        this.endingOffset = endingOffset;
    }

    public Map<TableId, TableChange> getTableSchemas() {
        return tableSchemas;
    }

    public void recordSchema(TableId tableId, TableChange latestTableChange) {
        this.tableSchemas.put(tableId, latestTableChange);
    }

    @Override
    public StreamSplit toSourceSplit() {
        final StreamSplit binlogSplit = split.asStreamSplit();
        return new StreamSplit(
                binlogSplit.splitId(),
                getStartingOffset(),
                getEndingOffset(),
                binlogSplit.asStreamSplit().getFinishedSnapshotSplitInfos(),
                getTableSchemas(),
                binlogSplit.getTotalFinishedSplitSize());
    }

    @Override
    public String toString() {
        return "StreamSplitState{"
                + "startingOffset="
                + startingOffset
                + ", endingOffset="
                + endingOffset
                + ", split="
                + split
                + '}';
    }
}
