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

package com.ververica.cdc.connectors.base.experimental.offset;

import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.offset.OffsetFactory;

import java.util.Map;

/** An offset factory class create {@link BinlogOffset} instance. */
public class BinlogOffsetFactory extends OffsetFactory {

    public BinlogOffsetFactory() {}

    @Override
    public Offset newOffset(Map<String, String> offset) {
        return new BinlogOffset(offset);
    }

    @Override
    public Offset newOffset(String filename, Long position) {
        return new BinlogOffset(filename, position);
    }

    @Override
    public Offset newOffset(Long position) {
        throw new FlinkRuntimeException("not supported create new Offset by Long position.");
    }

    @Override
    public Offset createInitialOffset() {
        return BinlogOffset.INITIAL_OFFSET;
    }

    @Override
    public Offset createNoStoppingOffset() {
        return BinlogOffset.NO_STOPPING_OFFSET;
    }
}
