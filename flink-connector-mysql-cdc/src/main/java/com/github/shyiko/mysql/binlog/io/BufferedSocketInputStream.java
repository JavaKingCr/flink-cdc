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

package com.github.shyiko.mysql.binlog.io;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Copied from https://github.com/osheroff/mysql-binlog-connector-java project to fix
 * https://github.com/ververica/flink-cdc-connectors/issues/460.
 *
 * <p>Line 50 and Line 70 ~ 72: Returns -1 means reach the end of InputStream. We should remove this
 * class after we bumped a higher mysql-binlog-connector-java version where the
 * https://github.com/osheroff/mysql-binlog-connector-java/issues/66 has been fixed.
 */
public class BufferedSocketInputStream extends FilterInputStream {

    private byte[] buffer;
    private int offset;
    private int limit;

    public BufferedSocketInputStream(InputStream in) {
        this(in, 512 * 1024);
    }

    public BufferedSocketInputStream(InputStream in, int bufferSize) {
        super(in);
        this.buffer = new byte[bufferSize];
    }

    @Override
    public int available() throws IOException {
        return limit == -1 ? in.available() : limit - offset + in.available();
    }

    @Override
    public int read() throws IOException {
        if (offset < limit) {
            return buffer[offset++] & 0xff;
        }
        offset = 0;
        limit = in.read(buffer, 0, buffer.length);
        return limit != -1 ? buffer[offset++] & 0xff : -1;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (offset >= limit) {
            if (len >= buffer.length) {
                return in.read(b, off, len);
            }
            offset = 0;
            limit = in.read(buffer, 0, buffer.length);
            if (limit == -1) {
                return limit;
            }
        }
        int bytesRemainingInBuffer = Math.min(len, limit - offset);
        System.arraycopy(buffer, offset, b, off, bytesRemainingInBuffer);
        offset += bytesRemainingInBuffer;
        return bytesRemainingInBuffer;
    }
}
