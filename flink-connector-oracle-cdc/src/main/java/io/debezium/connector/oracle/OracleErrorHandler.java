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

package io.debezium.connector.oracle;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

import java.io.IOException;
import java.sql.SQLRecoverableException;
import java.util.ArrayList;
import java.util.List;

/**
 * Copied from https://github.com/debezium/debezium project to fix
 * https://issues.redhat.com/browse/DBZ-4536 for 1.6.4.Final version.
 *
 * <p>This file is override to fix logger mining session stopped due to 'No more data to read from
 * socket' exception. please see more discussion under
 * https://github.com/debezium/debezium/pull/3118, We should remove this class since we bumped
 * higher debezium version after 1.8.1.Final where the issue has been fixed.
 */
public class OracleErrorHandler extends ErrorHandler {

    private static final List<String> retryOracleErrors = new ArrayList<>();
    private static final List<String> retryOracleMessageContainsTexts = new ArrayList<>();

    static {
        // Contents of this list should only be ORA-xxxxx errors
        // The error check uses starts-with semantics
        retryOracleErrors.add("ORA-03135"); // connection lost
        retryOracleErrors.add("ORA-12543"); // TNS:destination host unreachable
        retryOracleErrors.add("ORA-00604"); // error occurred at recursive SQL level 1
        retryOracleErrors.add("ORA-01089"); // Oracle immediate shutdown in progress
        retryOracleErrors.add("ORA-01333"); // Failed to establish LogMiner dictionary
        retryOracleErrors.add("ORA-01284"); // Redo/Archive log cannot be opened, likely locked
        retryOracleErrors.add(
                "ORA-26653"); // Apply DBZXOUT did not start properly and is currently in state
        // INITIAL
        retryOracleErrors.add("ORA-01291"); // missing logfile
        retryOracleErrors.add(
                "ORA-01327"); // failed to exclusively lock system dictionary as required BUILD
        retryOracleErrors.add("ORA-04030"); // out of process memory

        // Contents of this list should be any type of error message text
        // The error check uses case-insensitive contains semantics
        retryOracleMessageContainsTexts.add("No more data to read from socket");
    }

    public OracleErrorHandler(String logicalName, ChangeEventQueue<?> queue) {
        super(OracleConnector.class, logicalName, queue);
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        while (throwable != null) {
            // Always retry any recoverable error
            if (throwable instanceof SQLRecoverableException) {
                return true;
            }

            // If message is provided, run checks against it
            final String message = throwable.getMessage();
            if (message != null && message.length() > 0) {
                // Check Oracle error codes
                for (String errorCode : retryOracleErrors) {
                    if (message.startsWith(errorCode)) {
                        return true;
                    }
                }
                // Check Oracle error message texts
                for (String messageText : retryOracleMessageContainsTexts) {
                    if (message.toUpperCase().contains(messageText.toUpperCase())) {
                        return true;
                    }
                }
            }

            if (throwable.getCause() != null) {
                // We explicitly check this below the top-level error as we only want
                // certain nested exceptions to be retried, not if they're at the top
                final Throwable cause = throwable.getCause();
                if (cause instanceof IOException) {
                    return true;
                }
            }

            throwable = throwable.getCause();
        }
        return false;
    }
}
