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
import org.apache.druid.java.util.common.IAE;
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

class DoublesReservoirToPercentilesPostAggregatorTest {
    @Test
    public void testSerde() throws JsonProcessingException {
        final PostAggregator there = new DoublesReservoirToPercentilesPostAggregator(
                "post", new FieldAccessPostAggregator("field1", "reservoir"), new double[]{0, 0.5, 1});
        DefaultObjectMapper mapper = new DefaultObjectMapper();
        DoublesReservoirToPercentilesPostAggregator andBackAgain =
                mapper.readValue(mapper.writeValueAsString(there), DoublesReservoirToPercentilesPostAggregator.class);

        assertEquals(there, andBackAgain);
        Assertions.assertArrayEquals(there.getCacheKey(), andBackAgain.getCacheKey());
    }

    @Test
    public void testToString() {
        final PostAggregator postAgg = new DoublesReservoirToPercentilesPostAggregator(
                "post", new FieldAccessPostAggregator("field1", "reservoir"), new double[]{0, 0.5, 1});

        assertEquals(
                "DoublesReservoirToPercentilesPostAggregator{name='post', field=FieldAccessPostAggregator{name='field1', fieldName='reservoir'}, fractions=[0.0, 0.5, 1.0]}",
                postAgg.toString());
    }

    @Test
    public void testComparator() {
        IAE exception = Assertions.assertThrows(IAE.class, () -> {
            final PostAggregator postAgg = new DoublesReservoirToPercentilesPostAggregator(
                    "post", new FieldAccessPostAggregator("field1", "reservoir"), new double[]{0, 0.5, 1});
            postAgg.getComparator();
        });
        Assertions.assertEquals("Comparing arrays of percentiles is not supported", exception.getMessage());
    }

    @Test
    public void testEqualsAndHashCode() {
        EqualsVerifier.forClass(DoublesReservoirToPercentilesPostAggregator.class)
                .withNonnullFields("name", "field", "fractions")
                .usingGetClass()
                .verify();
    }

    @Test
    public void emptyReservoir() {
        final TestDoubleColumnSelectorImpl selector = new TestDoubleColumnSelectorImpl(null);

        try (final Aggregator agg = new DoublesReservoirBuildAggregator(selector, 8)) {
            final Map<String, Object> fields = new HashMap<>();
            fields.put("reservoir", agg.get());

            final PostAggregator postAgg = new DoublesReservoirToPercentilesPostAggregator(
                    "percentiles", new FieldAccessPostAggregator("field", "reservoir"), new double[]{0, 0.5, 1});

            final double[] percentiles = (double[]) postAgg.compute(fields);
            assertNotNull(percentiles);
            assertEquals(3, percentiles.length);
            assertTrue(Double.isNaN(percentiles[0]));
            assertTrue(Double.isNaN(percentiles[1]));
            assertTrue(Double.isNaN(percentiles[2]));
        }
    }

    @Test
    public void normalCase() {
        final double[] values = new double[]{1, 2, 3, 4, 5};
        final TestDoubleColumnSelectorImpl selector = new TestDoubleColumnSelectorImpl(values);

        try (final Aggregator agg = new DoublesReservoirBuildAggregator(selector, 8)) {
            for (int i = 0; i < values.length; i++) {
                agg.aggregate();
                selector.increment();
            }

            final Map<String, Object> fields = new HashMap<>();
            fields.put("reservoir", agg.get());

            final PostAggregator postAgg = new DoublesReservoirToPercentilesPostAggregator(
                    "percentiles", new FieldAccessPostAggregator("field", "reservoir"), new double[]{0, 0.5, 1});

            final double[] percentiles = (double[]) postAgg.compute(fields);
            assertNotNull(percentiles);
            assertEquals(3, percentiles.length);
            assertEquals(1.0, percentiles[0], 0);
            assertEquals(3.0, percentiles[1], 0);
            assertEquals(5.0, percentiles[2], 0);
        }
    }

    @Test
    public void testResultArraySignature() {
        final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                .dataSource("dummy")
                .intervals("2000/3000")
                .granularity(Granularities.HOUR)
                .aggregators(new DoublesReservoirAggregatorFactory("reservoir", "col", 8))
                .postAggregators(new DoublesReservoirToPercentilesPostAggregator(
                        "a", new FieldAccessPostAggregator("field", "reservoir"), new double[]{0, 0.5, 1}))
                .build();

        assertEquals(
                RowSignature.builder()
                        .addTimeColumn()
                        .add("reservoir", TYPE)
                        .add("a", ColumnType.DOUBLE_ARRAY)
                        .build(),
                new TimeseriesQueryQueryToolChest().resultArraySignature(query));
    }
}
