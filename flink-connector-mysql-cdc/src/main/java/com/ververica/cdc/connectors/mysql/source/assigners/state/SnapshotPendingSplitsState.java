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

package com.ververica.cdc.connectors.mysql.source.assigners.state;

import com.ververica.cdc.connectors.mysql.source.assigners.AssignerStatus;
import com.ververica.cdc.connectors.mysql.source.enumerator.MySqlSourceEnumerator;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.reader.MySqlSplitReader;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import io.debezium.relational.TableId;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** A {@link PendingSplitsState} for pending snapshot splits. */
public class SnapshotPendingSplitsState extends PendingSplitsState {

    /** The tables in the checkpoint. */
    private final List<TableId> remainingTables;

    /**
     * The paths that are no longer in the enumerator checkpoint, but have been processed before and
     * should this be ignored. Relevant only for sources in continuous monitoring mode.
     */
    private final List<TableId> alreadyProcessedTables;

    /** The splits in the checkpoint. */
    private final List<MySqlSnapshotSplit> remainingSplits;

    /**
     * The snapshot splits that the {@link MySqlSourceEnumerator} has assigned to {@link
     * MySqlSplitReader}s.
     */
    private final Map<String, MySqlSnapshotSplit> assignedSplits;

    /**
     * The offsets of finished (snapshot) splits that the {@link MySqlSourceEnumerator} has received
     * from {@link MySqlSplitReader}s.
     */
    private final Map<String, BinlogOffset> splitFinishedOffsets;

    /** The {@link AssignerStatus} that indicates the snapshot assigner status. */
    private final AssignerStatus assignerStatus;

    /** Whether the table identifier is case-sensitive. */
    private final boolean isTableIdCaseSensitive;

    /** Whether the remaining tables are keep when snapshot state. */
    private final boolean isRemainingTablesCheckpointed;

    public SnapshotPendingSplitsState(
            List<TableId> alreadyProcessedTables,
            List<MySqlSnapshotSplit> remainingSplits,
            Map<String, MySqlSnapshotSplit> assignedSplits,
            Map<String, BinlogOffset> splitFinishedOffsets,
            AssignerStatus assignerStatus,
            List<TableId> remainingTables,
            boolean isTableIdCaseSensitive,
            boolean isRemainingTablesCheckpointed) {
        this.alreadyProcessedTables = alreadyProcessedTables;
        this.remainingSplits = remainingSplits;
        this.assignedSplits = assignedSplits;
        this.splitFinishedOffsets = splitFinishedOffsets;
        this.assignerStatus = assignerStatus;
        this.remainingTables = remainingTables;
        this.isTableIdCaseSensitive = isTableIdCaseSensitive;
        this.isRemainingTablesCheckpointed = isRemainingTablesCheckpointed;
    }

    public List<TableId> getAlreadyProcessedTables() {
        return alreadyProcessedTables;
    }

    public List<MySqlSnapshotSplit> getRemainingSplits() {
        return remainingSplits;
    }

    public Map<String, MySqlSnapshotSplit> getAssignedSplits() {
        return assignedSplits;
    }

    public Map<String, BinlogOffset> getSplitFinishedOffsets() {
        return splitFinishedOffsets;
    }

    public AssignerStatus getSnapshotAssignerStatus() {
        return assignerStatus;
    }

    public List<TableId> getRemainingTables() {
        return remainingTables;
    }

    public boolean isTableIdCaseSensitive() {
        return isTableIdCaseSensitive;
    }

    public boolean isRemainingTablesCheckpointed() {
        return isRemainingTablesCheckpointed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SnapshotPendingSplitsState)) {
            return false;
        }
        SnapshotPendingSplitsState that = (SnapshotPendingSplitsState) o;
        return assignerStatus == that.assignerStatus
                && isTableIdCaseSensitive == that.isTableIdCaseSensitive
                && isRemainingTablesCheckpointed == that.isRemainingTablesCheckpointed
                && Objects.equals(remainingTables, that.remainingTables)
                && Objects.equals(alreadyProcessedTables, that.alreadyProcessedTables)
                && Objects.equals(remainingSplits, that.remainingSplits)
                && Objects.equals(assignedSplits, that.assignedSplits)
                && Objects.equals(splitFinishedOffsets, that.splitFinishedOffsets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                remainingTables,
                alreadyProcessedTables,
                remainingSplits,
                assignedSplits,
                splitFinishedOffsets,
                assignerStatus,
                isTableIdCaseSensitive,
                isRemainingTablesCheckpointed);
    }

    @Override
    public String toString() {
        return "SnapshotPendingSplitsState{"
                + "remainingTables="
                + remainingTables
                + ", alreadyProcessedTables="
                + alreadyProcessedTables
                + ", remainingSplits="
                + remainingSplits
                + ", assignedSplits="
                + assignedSplits
                + ", splitFinishedOffsets="
                + splitFinishedOffsets
                + ", assignerStatus="
                + assignerStatus
                + ", isTableIdCaseSensitive="
                + isTableIdCaseSensitive
                + ", isRemainingTablesCheckpointed="
                + isRemainingTablesCheckpointed
                + '}';
    }
}
