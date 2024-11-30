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

import static bi.deep.aggregation.percentiles.aggregator.DoublesReservoirMergeAggregatorFactory.TYPE_NAME;

import bi.deep.aggregation.percentiles.reservoir.DoublesReservoir;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.NilColumnValueSelector;

@JsonTypeName(TYPE_NAME)
public class DoublesReservoirMergeAggregatorFactory extends DoublesReservoirAggregatorFactory {
    public static final String TYPE_NAME = "doubleReservoirMerge";
    private static final byte CACHE_ID = 0x61;

    @JsonCreator
    public DoublesReservoirMergeAggregatorFactory(
            @JsonProperty("name") String name, @JsonProperty("maxReservoirSize") int maxReservoirSize) {
        super(name, name, maxReservoirSize);
    }

    @Override
    public Aggregator factorize(ColumnSelectorFactory metricFactory) {
        @SuppressWarnings("unchecked")
        final ColumnValueSelector<DoublesReservoir> selector = metricFactory.makeColumnValueSelector(getFieldName());

        return selector instanceof NilColumnValueSelector
                ? new NoopReservoirAggregator()
                : new DoublesReservoirMergeAggregator(selector, getMaxReservoirSize());
    }

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory) {
        @SuppressWarnings("unchecked")
        final ColumnValueSelector<DoublesReservoir> selector = metricFactory.makeColumnValueSelector(getFieldName());

        return selector instanceof NilColumnValueSelector
                ? new NoopReservoirBufferAggregator()
                : new DoublesReservoirMergeBufferAggregator(selector, getMaxReservoirSize());
    }

    @Override
    public AggregatorFactory withName(String newName) {
        return new DoublesReservoirMergeAggregatorFactory(newName, getMaxReservoirSize());
    }

    @Override
    public byte getCacheId() {
        return CACHE_ID;
    }
}
