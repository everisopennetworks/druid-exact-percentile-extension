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
package bi.deep.aggregation.percentiles.aggregator;

import bi.deep.aggregation.percentiles.reservoir.DoublesReservoir;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.ColumnValueSelector;

public class DoublesReservoirBuildAggregator implements Aggregator {
    private final DoublesReservoir reservoir;
    private final ColumnValueSelector<Double> selector;

    public DoublesReservoirBuildAggregator(final ColumnValueSelector<Double> selector, int maxSize) {
        this.selector = selector;
        this.reservoir = new DoublesReservoir(maxSize);
    }

    @Override
    public synchronized void aggregate() {
        if (!selector.isNull()) {
            reservoir.add(selector.getDouble());
        }
    }

    @Override
    public synchronized Object get() {
        return reservoir.get();
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
        // NoOp
    }
}
