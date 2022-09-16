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

package com.ververica.cdc.connectors.base.source.meta.offset;

import org.apache.flink.annotation.Experimental;

import org.apache.kafka.connect.errors.ConnectException;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * A structure describes a fine-grained offset in a binlog event including binlog position and gtid
 * set etc.
 *
 * <p>This structure can also be used to deal the binlog event in transaction, a transaction may
 * contain multiple change events, and each change event may contain multiple rows. When restart
 * from a specific {@link Offset}, we need to skip the processed change events and the processed
 * rows.
 */
@Experimental
public abstract class Offset implements Comparable<Offset>, Serializable {

    private static final long serialVersionUID = 1L;

    protected Map<String, String> offset;

    public Map<String, String> getOffset() {
        return offset;
    }

    protected long longOffsetValue(Map<String, ?> values, String key) {
        Object obj = values.get(key);
        if (obj == null) {
            return 0L;
        }
        if (obj instanceof Number) {
            return ((Number) obj).longValue();
        }
        try {
            return Long.parseLong(obj.toString());
        } catch (NumberFormatException e) {
            throw new ConnectException(
                    "Source offset '"
                            + key
                            + "' parameter value "
                            + obj
                            + " could not be converted to a long");
        }
    }

    public boolean isAtOrBefore(Offset that) {
        return this.compareTo(that) <= 0;
    }

    public boolean isBefore(Offset that) {
        return this.compareTo(that) < 0;
    }

    public boolean isAtOrAfter(Offset that) {
        return this.compareTo(that) >= 0;
    }

    public boolean isAfter(Offset that) {
        return this.compareTo(that) > 0;
    }

    @Override
    public String toString() {
        return offset.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Offset)) {
            return false;
        }
        Offset that = (Offset) o;
        return offset.equals(that.offset);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(offset);
    }
}
