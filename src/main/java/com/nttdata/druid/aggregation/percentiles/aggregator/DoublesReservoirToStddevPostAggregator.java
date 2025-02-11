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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@JsonTypeName("doublesReservoirToStddev")
public class DoublesReservoirToStddevPostAggregator implements PostAggregator {
    private static final byte CACHE_ID = 0x64;

    private final String name;
    private final PostAggregator field;

    @JsonCreator
    public DoublesReservoirToStddevPostAggregator(
            @JsonProperty("name") final String name,
            @JsonProperty("field") final PostAggregator field) {
        this.name = Preconditions.checkNotNull(name, "name is null");
        this.field = Preconditions.checkNotNull(field, "field is null");
    }

    @Nullable
    @Override
    public ColumnType getType(ColumnInspector signature) {
        return ColumnType.DOUBLE;
    }

    @Override
    public Set<String> getDependentFields() {
        return field.getDependentFields();
    }

    @Override
    public Comparator<Double> getComparator() {
        return Doubles::compare;
    }

    @Nullable
    @Override
    public Object compute(Map<String, Object> combinedAggregators) {
        final Object compute = getField().compute(combinedAggregators);
        final DoublesReservoir reservoir = DoublesReservoir.deserialize(compute);

        return reservoir.getStddev();
    }

    @JsonProperty
    @Override
    public String getName() {
        return name;
    }

    @JsonProperty
    public PostAggregator getField() {
        return field;
    }

    @Override
    public PostAggregator decorate(Map<String, AggregatorFactory> map) {
        return this;
    }

    @Override
    public byte[] getCacheKey() {
        return new CacheKeyBuilder(CACHE_ID)
                .appendCacheable(field)
                .build();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
                + "{" + "name='" + name + '\''
                + ", field=" + field
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final DoublesReservoirToStddevPostAggregator that = (DoublesReservoirToStddevPostAggregator) o;
        return Objects.equals(name, that.name)
                && Objects.equals(field, that.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, field);
    }
}
