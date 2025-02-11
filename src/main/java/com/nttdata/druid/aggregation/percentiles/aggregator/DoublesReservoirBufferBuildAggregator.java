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
package com.nttdata.druid.aggregation.percentiles.aggregator;

import com.nttdata.druid.aggregation.percentiles.reservoir.DoublesReservoir;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.nio.ByteBuffer;
import java.util.IdentityHashMap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnValueSelector;

public class DoublesReservoirBufferBuildAggregator implements BufferAggregator {
    private final ColumnValueSelector<?> selector;
    private final IdentityHashMap<ByteBuffer, Int2ObjectMap<DoublesReservoir>> cache = new IdentityHashMap<>();
    private final int maxSize;

    public DoublesReservoirBufferBuildAggregator(ColumnValueSelector<?> selector, int maxReservoirSize) {
        this.selector = Preconditions.checkNotNull(selector);
        this.maxSize = maxReservoirSize;
    }

    @Override
    public void init(ByteBuffer buffer, int position) {
        final DoublesReservoir emptyReservoir = new DoublesReservoir(maxSize);
        addToCache(buffer, position, emptyReservoir);
    }

    @Override
    public void aggregate(ByteBuffer buffer, int position) {
        final Object obj = selector.getObject();

        if (obj == null) {
            return;
        }

        final DoublesReservoir doublesReservoir = get(buffer, position);

        if (obj instanceof Number) {
            doublesReservoir.accept(((Number) obj).doubleValue());
        } else if (obj instanceof DoublesReservoir) {
            doublesReservoir.mergeWith((DoublesReservoir) obj);
        } else {
            throw new IAE(
                    "Expected a number or an instance of DoublesReservoir, but received [%s] of type [%s]",
                    obj, obj.getClass());
        }
    }

    @Override
    public DoublesReservoir get(final ByteBuffer buffer, final int position) {
        return cache.get(buffer).get(position);
    }

    @Override
    public float getFloat(final ByteBuffer buffer, final int position) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public long getLong(final ByteBuffer buffer, final int position) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void close() {
        cache.clear();
    }

    @Override
    public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer) {
        final DoublesReservoir doublesReservoir = get(oldBuffer, oldPosition);
        addToCache(newBuffer, newPosition, doublesReservoir);

        final Int2ObjectMap<DoublesReservoir> map = cache.get(oldBuffer);
        map.remove(oldPosition);

        if (map.isEmpty()) {
            cache.remove(oldBuffer);
        }
    }

    private void addToCache(final ByteBuffer buffer, final int position, final DoublesReservoir histogram) {
        cache.computeIfAbsent(buffer, b -> new Int2ObjectOpenHashMap<>()).put(position, histogram);
    }
}
