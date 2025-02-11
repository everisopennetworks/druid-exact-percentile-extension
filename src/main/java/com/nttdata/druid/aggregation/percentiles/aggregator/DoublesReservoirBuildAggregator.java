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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.nttdata.druid.aggregation.percentiles.reservoir.DoublesReservoir;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.ColumnValueSelector;

public class DoublesReservoirBuildAggregator implements Aggregator {
    private final ColumnValueSelector<?> selector;

    @GuardedBy("this")
    private DoublesReservoir reservoir;

    public DoublesReservoirBuildAggregator(final ColumnValueSelector<?> selector, int maxSize) {
        this.selector = selector;
        this.reservoir = new DoublesReservoir(maxSize);
    }

    @Override
    public synchronized void aggregate() {
        Object obj = selector.getObject();

        if (obj == null) {
            return;
        }

        if (obj instanceof Number) {
            this.reservoir.accept(((Number) obj).doubleValue());
        } else if (obj instanceof DoublesReservoir) {
            this.reservoir.mergeWith((DoublesReservoir) obj);
        } else {
            throw new IAE(
                    "Expected a number or an instance of DoublesReservoir, but received [%s] of type [%s]",
                    obj, obj.getClass());
        }
    }

    @Override
    public synchronized Object get() {
        return reservoir;
    }

    @Override
    public float getFloat() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public long getLong() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public double getDouble() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public synchronized void close() {
        reservoir = null;
    }
}
