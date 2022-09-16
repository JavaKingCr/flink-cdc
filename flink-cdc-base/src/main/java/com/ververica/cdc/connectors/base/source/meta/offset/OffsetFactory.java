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

import java.io.Serializable;
import java.util.Map;

/** An offset factory class create {@link Offset} instance. */
public abstract class OffsetFactory implements Serializable {
    private static final long serialVersionUID = 1L;

    public OffsetFactory() {}

    public abstract Offset newOffset(Map<String, String> offset);

    public abstract Offset newOffset(String filename, Long position);

    public abstract Offset newOffset(Long position);

    public abstract Offset createInitialOffset();

    public abstract Offset createNoStoppingOffset();
}
