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

package com.ververica.cdc.connectors.mysql.source.assigners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;

/**
 * The state of split assigner finite state machine, tips: we use word status instead of word state
 * to avoid conflict with Flink state keyword. The assigner finite state machine goes this way.
 *
 * <pre>
 *        INITIAL_ASSIGNING(start)
 *              |
 *              |
 *          onFinish()
 *              |
 *              ↓
 *    INITIAL_ASSIGNING_FINISHED(end)
 *              |
 *              |
 *        suspend() // found newly added tables
 *              |
 *              ↓
 *          SUSPENDED --- wakeup() --→ NEWLY_ADDED_ASSIGNING --- onFinish() --→ NEWLY_ADDED_ASSIGNING_FINISHED(end)
 *              ↑                                                                  |
 *              |                                                                  |
 *              |----------------- suspend() //found newly added tables -----------|
 * </pre>
 */
public enum AssignerStatus {
    INITIAL_ASSIGNING(0) {
        @Override
        public AssignerStatus getNextStatus() {
            return INITIAL_ASSIGNING_FINISHED;
        }

        @Override
        public AssignerStatus onFinish() {
            LOG.info(
                    "Assigner status changes from INITIAL_ASSIGNING to INITIAL_ASSIGNING_FINISHED");
            return this.getNextStatus();
        }
    },
    INITIAL_ASSIGNING_FINISHED(1) {
        @Override
        public AssignerStatus getNextStatus() {
            return SUSPENDED;
        }

        @Override
        public AssignerStatus suspend() {
            LOG.info("Assigner status changes from INITIAL_ASSIGNING_FINISHED to SUSPENDED");
            return this.getNextStatus();
        }
    },
    SUSPENDED(2) {
        @Override
        public AssignerStatus getNextStatus() {
            return NEWLY_ADDED_ASSIGNING;
        }

        @Override
        public AssignerStatus wakeup() {
            LOG.info("Assigner status changes from SUSPENDED to NEWLY_ADDED_ASSIGNING");
            return this.getNextStatus();
        }
    },
    NEWLY_ADDED_ASSIGNING(3) {
        @Override
        public AssignerStatus getNextStatus() {
            return NEWLY_ADDED_ASSIGNING_FINISHED;
        }

        @Override
        public AssignerStatus onFinish() {
            LOG.info(
                    "Assigner status changes from NEWLY_ADDED_ASSIGNING to NEWLY_ADDED_ASSIGNING_FINISHED");
            return this.getNextStatus();
        }
    },
    NEWLY_ADDED_ASSIGNING_FINISHED(4) {
        @Override
        public AssignerStatus getNextStatus() {
            return SUSPENDED;
        }

        @Override
        public AssignerStatus suspend() {
            LOG.info("Assigner status changes from NEWLY_ADDED_ASSIGNING_FINISHED to SUSPENDED");
            return this.getNextStatus();
        }
    };

    private static final Logger LOG = LoggerFactory.getLogger(AssignerStatus.class);
    private final int statusCode;

    AssignerStatus(int statusCode) {
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public abstract AssignerStatus getNextStatus();

    public AssignerStatus onFinish() {
        throw new IllegalStateException(
                format(
                        "Invalid call, assigner under %s state can not call onFinish()",
                        fromStatusCode(this.getStatusCode())));
    }

    public AssignerStatus suspend() {
        throw new IllegalStateException(
                format(
                        "Invalid call, assigner under %s state can not call suspend()",
                        fromStatusCode(this.getStatusCode())));
    }

    public AssignerStatus wakeup() {
        throw new IllegalStateException(
                format(
                        "Invalid call, assigner under %s state can not call wakeup()",
                        fromStatusCode(this.getStatusCode())));
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    /** Gets the {@link AssignerStatus} from status code. */
    public static AssignerStatus fromStatusCode(int statusCode) {
        switch (statusCode) {
            case 0:
                return INITIAL_ASSIGNING;
            case 1:
                return INITIAL_ASSIGNING_FINISHED;
            case 2:
                return SUSPENDED;
            case 3:
                return NEWLY_ADDED_ASSIGNING;
            case 4:
                return NEWLY_ADDED_ASSIGNING_FINISHED;
            default:
                throw new IllegalStateException(
                        format(
                                "Invalid status code %s,the valid code range is [0, 4]",
                                statusCode));
        }
    }

    /** Returns whether the split assigner state is suspended. */
    public static boolean isSuspended(AssignerStatus assignerStatus) {
        return assignerStatus == SUSPENDED;
    }

    /**
     * Returns whether the split assigner has assigned all snapshot splits, which indicates there is
     * no more splits and all records of splits have been completely processed in the pipeline.
     */
    public static boolean isAssigningFinished(AssignerStatus assignerStatus) {
        return assignerStatus == INITIAL_ASSIGNING_FINISHED
                || assignerStatus == NEWLY_ADDED_ASSIGNING_FINISHED;
    }

    /** Returns whether the split assigner is assigning snapshot splits. */
    public static boolean isAssigning(AssignerStatus assignerStatus) {
        return assignerStatus == INITIAL_ASSIGNING || assignerStatus == NEWLY_ADDED_ASSIGNING;
    }

    /** Returns whether the split assigner is assigning newly added snapshot splits. */
    public static boolean isNewlyAddedAssigning(AssignerStatus assignerStatus) {
        return assignerStatus == NEWLY_ADDED_ASSIGNING;
    }

    /** Returns whether the split assigner has finished its initial tables assignment. */
    public static boolean isInitialAssigningFinished(AssignerStatus assignerStatus) {
        return assignerStatus == INITIAL_ASSIGNING_FINISHED;
    }

    /** Returns whether the split assigner has finished its newly added tables assignment. */
    public static boolean isNewlyAddedAssigningFinished(AssignerStatus assignerStatus) {
        return assignerStatus == NEWLY_ADDED_ASSIGNING_FINISHED;
    }
}
