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

import static bi.deep.DoublesReservoirModule.TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.TestDoubleColumnSelectorImpl;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.jupiter.api.Test;

class DoublesReservoirAggregatorFactoryTest {
    @Test
    void testEquals() {
        EqualsVerifier.forClass(DoublesReservoirAggregatorFactory.class)
                .withNonnullFields("name", "fieldName", "maxReservoirSize")
                .usingGetClass()
                .verify();
    }

    @Test
    void testSerde() throws IOException {
        final ObjectMapper mapper = new DefaultObjectMapper();
        mapper.registerSubtypes(DoublesReservoirAggregatorFactory.class);

        final DoublesReservoirAggregatorFactory factory =
                new DoublesReservoirAggregatorFactory("myFactory", "myField", 1024);
        final byte[] json = mapper.writeValueAsBytes(factory);
        final DoublesReservoirAggregatorFactory fromJson =
                (DoublesReservoirAggregatorFactory) mapper.readValue(json, AggregatorFactory.class);
        assertEquals(factory, fromJson);
    }

    @Test
    void testGuessAggregatorHeapFootprint() {
        DoublesReservoirAggregatorFactory factory = new DoublesReservoirAggregatorFactory("myFactory", "myField", 128);

        assertEquals(1024, factory.guessAggregatorHeapFootprint(1));
        assertEquals(1024, factory.guessAggregatorHeapFootprint(100));
        assertEquals(1024, factory.guessAggregatorHeapFootprint(1000));
        assertEquals(1024, factory.guessAggregatorHeapFootprint(1_000_000_000_000L));
    }

    @Test
    void testMaxIntermediateSize() {
        DoublesReservoirAggregatorFactory factory = new DoublesReservoirAggregatorFactory("myFactory", "myField", 128);
        assertEquals(1024, factory.getMaxIntermediateSize());
    }

    @Test
    void testResultArraySignature() {
        final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                .dataSource("dummy")
                .intervals("2000/3000")
                .granularity(Granularities.HOUR)
                .aggregators(
                        new CountAggregatorFactory("count"),
                        new DoublesReservoirAggregatorFactory("doublesReservoir", "col", 8))
                .postAggregators(
                        new FieldAccessPostAggregator("doublesReservoir-access", "doublesReservoir"),
                        new FinalizingFieldAccessPostAggregator("doublesReservoir-finalize", "doublesReservoir"))
                .build();

        assertEquals(
                RowSignature.builder()
                        .addTimeColumn()
                        .add("count", ColumnType.LONG)
                        .add("doublesReservoir", TYPE)
                        .add("doublesReservoir-access", TYPE)
                        .add("doublesReservoir-finalize", TYPE)
                        .build(),
                new TimeseriesQueryQueryToolChest().resultArraySignature(query));
    }

    @Test
    void testNullReservoir() {
        final DoublesReservoirAggregatorFactory factory =
                new DoublesReservoirAggregatorFactory("myFactory", "myField", 1024);
        final double[] values = new double[] {1, 2, 3, 4, 5, 6};
        final TestDoubleColumnSelectorImpl selector = new TestDoubleColumnSelectorImpl(values);

        try (final Aggregator agg1 = new DoublesReservoirBuildAggregator(selector, 8)) {
            assertNotNull(factory.combine(null, agg1.get()));
            assertNotNull(factory.combine(agg1.get(), null));

            AggregateCombiner<?> ac = factory.makeAggregateCombiner();
            ac.fold(new TestDoublesReservoirColumnValueSelector());
            assertNotNull(ac.getObject());
        }
    }
}
