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

import com.fasterxml.jackson.core.JsonProcessingException;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.TestDoubleColumnSelectorImpl;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.nttdata.druid.DoublesReservoirModule.TYPE;
import static org.junit.jupiter.api.Assertions.*;

class DoublesReservoirToStddevPostAggregatorTest {
    @Test
    void testSerde() throws JsonProcessingException {
        final PostAggregator there = new DoublesReservoirToStddevPostAggregator(
                "post", new FieldAccessPostAggregator("field1", "reservoir"));
        DefaultObjectMapper mapper = new DefaultObjectMapper();
        DoublesReservoirToStddevPostAggregator andBackAgain =
                mapper.readValue(mapper.writeValueAsString(there), DoublesReservoirToStddevPostAggregator.class);

        assertEquals(there, andBackAgain);
        Assertions.assertArrayEquals(there.getCacheKey(), andBackAgain.getCacheKey());
    }

    @Test
    void testToString() {
        final PostAggregator postAgg = new DoublesReservoirToStddevPostAggregator(
                "post", new FieldAccessPostAggregator("field1", "reservoir"));

        assertEquals(
                "DoublesReservoirToStddevPostAggregator{name='post', field=FieldAccessPostAggregator{name='field1', fieldName='reservoir'}}",
                postAgg.toString());
    }

    @Test
    public void emptyReservoir() {
        final TestDoubleColumnSelectorImpl selector = new TestDoubleColumnSelectorImpl(null);

        try (final Aggregator agg = new DoublesReservoirBuildAggregator(selector, 8)) {
            final Map<String, Object> fields = new HashMap<>();
            fields.put("reservoir", agg.get());

            final PostAggregator postAgg = new DoublesReservoirToStddevPostAggregator(
                    "stddev", new FieldAccessPostAggregator("field", "reservoir"));

            final Double stddev = (Double) postAgg.compute(fields);
            assertNotNull(stddev);

            assertTrue(Double.isNaN(stddev));
        }
    }

    @Test
    public void normalCase() {
        final double[] values = new double[] {1, 2, 3, 4, 5};
        final TestDoubleColumnSelectorImpl selector = new TestDoubleColumnSelectorImpl(values);

        try (final Aggregator agg = new DoublesReservoirBuildAggregator(selector, 8)) {
            for (int i = 0; i < values.length; i++) {
                agg.aggregate();
                selector.increment();
            }

            final Map<String, Object> fields = new HashMap<>();
            fields.put("reservoir", agg.get());

            final PostAggregator postAgg = new DoublesReservoirToStddevPostAggregator(
                    "stddev", new FieldAccessPostAggregator("field", "reservoir"));

            final Double stddev = (Double) postAgg.compute(fields);
            assertNotNull(stddev);
            assertEquals(Math.sqrt(2.5), stddev, 0);
        }
    }

    @Test
    public void testResultArraySignature() {
        final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                .dataSource("dummy")
                .intervals("2000/3000")
                .granularity(Granularities.HOUR)
                .aggregators(new DoublesReservoirAggregatorFactory("reservoir", "col", 8))
                .postAggregators(new DoublesReservoirToStddevPostAggregator(
                        "a", new FieldAccessPostAggregator("field", "reservoir")))
                .build();

        assertEquals(
                RowSignature.builder()
                        .addTimeColumn()
                        .add("reservoir", TYPE)
                        .add("a", ColumnType.DOUBLE)
                        .build(),
                new TimeseriesQueryQueryToolChest().resultArraySignature(query));
    }

    @Test
    void testEqualsAndHashCode() {
        EqualsVerifier.forClass(DoublesReservoirToStddevPostAggregator.class)
                .withNonnullFields("name", "field")
                .usingGetClass()
                .verify();
    }
}
