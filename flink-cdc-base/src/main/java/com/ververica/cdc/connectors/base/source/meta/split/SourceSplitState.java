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

/** State of the reader, essentially a mutable version of the {@link SourceSplitBase}. */
public abstract class SourceSplitState {

    protected final SourceSplitBase split;

    public SourceSplitState(SourceSplitBase split) {
        this.split = split;
    }

    /** Checks whether this split state is a snapshot split state. */
    public final boolean isSnapshotSplitState() {
        return getClass() == SnapshotSplitState.class;
    }

    /** Checks whether this split state is a binlog split state. */
    public final boolean isStreamSplitState() {
        return getClass() == StreamSplitState.class;
    }

    /** Casts this split state into a {@link SnapshotSplitState}. */
    public final SnapshotSplitState asSnapshotSplitState() {
        return (SnapshotSplitState) this;
    }

    /** Casts this split state into a {@link StreamSplitState}. */
    public final StreamSplitState asStreamSplitState() {
        return (StreamSplitState) this;
    }

    /** Use the current split state to create a new MySqlSplit. */
    public abstract SourceSplitBase toSourceSplit();
}
