/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.nttdata.druid.aggregation.percentiles.reservoir;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.data.ObjectStrategy;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class DoublesReservoirObjectStrategy implements ObjectStrategy<DoublesReservoir> {
    private static final byte[] EMPTY_BYTES = new byte[]{};

    @Override
    public int compare(final DoublesReservoir s1, final DoublesReservoir s2) {
        return DoublesReservoir.COMPARATOR.compare(s1, s2);
    }

    @Override
    public DoublesReservoir fromByteBuffer(final ByteBuffer buffer, final int numBytes) {
        if (numBytes == 0) {
            return DoublesReservoir.EMPTY;
        }

        final byte[] data = new byte[numBytes];
        buffer.get(data);

        try {
            String json = new String(data, StandardCharsets.UTF_8);
            return DoublesReservoirUtils.readJson(json);
        } catch (IOException e) {
            throw new IAE("Unable to read from byte buffer", e);
        }
    }

    @Override
    public Class<DoublesReservoir> getClazz() {
        return DoublesReservoir.class;
    }

    @Override
    public byte[] toBytes(@Nullable DoublesReservoir reservoir) {
        if (reservoir == null) {
            return EMPTY_BYTES;
        }

        try {
            return DoublesReservoirUtils.convertToJson(reservoir).getBytes(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IAE("Unable to convert to byte array", e);
        }
    }
}
