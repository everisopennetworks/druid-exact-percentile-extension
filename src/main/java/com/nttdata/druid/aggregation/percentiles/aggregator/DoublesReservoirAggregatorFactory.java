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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.nttdata.druid.aggregation.percentiles.reservoir.DoublesReservoir;
import org.apache.commons.lang.StringUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.*;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import static com.nttdata.druid.DoublesReservoirModule.TYPE;
import static com.nttdata.druid.DoublesReservoirModule.TYPE_NAME;

@JsonTypeName(TYPE_NAME)
public class DoublesReservoirAggregatorFactory extends AggregatorFactory {
    private static final byte CACHE_ID = 0x60;

    private final String name;
    private final String fieldName;
    private final int maxReservoirSize;

    @JsonCreator
    public DoublesReservoirAggregatorFactory(
            @JsonProperty("name") final String name,
            @JsonProperty("fieldName") final String fieldName,
            @JsonProperty("maxReservoirSize") final Integer maxReservoirSize) {

        if (StringUtils.isBlank(name)) {
            throw new IAE("Must have a valid, non-null aggregator name");
        }
        if (StringUtils.isBlank(fieldName)) {
            throw new IAE("Parameter fieldName must be specified");
        }
        if (maxReservoirSize <= 0) {
            throw new IAE("Parameter maxReservoirSize must be specified and greater than 0");
        }

        this.fieldName = fieldName;
        this.name = name;
        this.maxReservoirSize = maxReservoirSize;
    }

    @Override
    public Aggregator factorize(final ColumnSelectorFactory metricFactory) {
        final ColumnValueSelector<?> selector = metricFactory.makeColumnValueSelector(getFieldName());

        return selector instanceof NilColumnValueSelector
                ? new NoopReservoirAggregator()
                : new DoublesReservoirBuildAggregator(selector, getMaxReservoirSize());
    }

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory) {
        final ColumnValueSelector<?> selector = metricFactory.makeColumnValueSelector(getFieldName());

        return selector instanceof NilColumnValueSelector
                ? new NoopReservoirBufferAggregator()
                : new DoublesReservoirBufferBuildAggregator(selector, getMaxReservoirSize());
    }

    @Override
    public Comparator<DoublesReservoir> getComparator() {
        return DoublesReservoir.COMPARATOR;
    }

    @Nullable
    @Override
    public Object combine(@Nullable Object lhs, @Nullable Object rhs) {
        if (lhs == null) {
            return rhs;
        }
        if (rhs == null) {
            return lhs;
        }

        return DoublesReservoir.deserialize(lhs).mergeWith(DoublesReservoir.deserialize(rhs));
    }

    @Override
    public AggregatorFactory getCombiningFactory() {
        return new DoublesReservoirAggregatorFactory(getName(), getName(), getMaxReservoirSize());
    }

    @SuppressWarnings("rawtypes")
    @Override
    public AggregateCombiner makeAggregateCombiner() {
        return new ObjectAggregateCombiner<DoublesReservoir>() {
            private DoublesReservoir combined = null;

            @Override
            public void reset(final ColumnValueSelector selector) {
                combined = null;
                fold(selector);
            }

            @Override
            public void fold(final ColumnValueSelector selector) {
                DoublesReservoir other = (DoublesReservoir) selector.getObject();

                if (other == null) {
                    return;
                }

                if (combined == null) {
                    combined = new DoublesReservoir(maxReservoirSize);
                }

                combined.mergeWith(other);
            }

            @Override
            public DoublesReservoir getObject() {
                return combined;
            }

            @Override
            public Class<DoublesReservoir> classOfObject() {
                return DoublesReservoir.class;
            }
        };
    }

    @Override
    public Object deserialize(Object serializedObject) {
        return DoublesReservoir.deserialize(serializedObject);
    }

    @Nullable
    @Override
    public Object finalizeComputation(@Nullable Object object) {
        return object;
    }

    @Override
    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public String getFieldName() {
        return fieldName;
    }

    @JsonProperty
    public int getMaxReservoirSize() {
        return maxReservoirSize;
    }

    @Override
    public List<String> requiredFields() {
        return Collections.singletonList(fieldName);
    }

    @Override
    public int getMaxIntermediateSize() {
        return maxReservoirSize * Double.BYTES;
    }

    @Override
    public AggregatorFactory withName(String newName) {
        return new DoublesReservoirAggregatorFactory(newName, getFieldName(), getMaxReservoirSize());
    }

    @Override
    public ColumnType getIntermediateType() {
        return TYPE;
    }

    @Override
    public ColumnType getResultType() {
        return TYPE;
    }

    @Override
    public byte[] getCacheKey() {
        return new CacheKeyBuilder(getCacheId())
                .appendString(name)
                .appendString(fieldName)
                .appendInt(maxReservoirSize)
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final DoublesReservoirAggregatorFactory that = (DoublesReservoirAggregatorFactory) o;

        return Objects.equals(name, that.name)
                && Objects.equals(fieldName, that.fieldName)
                && maxReservoirSize == that.maxReservoirSize;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, fieldName, maxReservoirSize);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{"
                + "name=" + name
                + ", fieldName=" + fieldName
                + ", maxReservoirSize=" + maxReservoirSize
                + "}";
    }

    protected byte getCacheId() {
        return CACHE_ID;
    }
}
