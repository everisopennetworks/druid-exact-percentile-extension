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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.junit.jupiter.api.Test;

public class DoublesReservoirMergeAggregatorFactoryTest {
    @Test
    public void testEquals() {
        EqualsVerifier.forClass(DoublesReservoirMergeAggregatorFactory.class)
                .withNonnullFields("name", "maxReservoirSize")
                .usingGetClass()
                .verify();
    }

    @Test
    public void testSerde() throws IOException {
        final ObjectMapper mapper = new DefaultObjectMapper();
        mapper.registerSubtypes(DoublesReservoirMergeAggregatorFactory.class);

        final DoublesReservoirMergeAggregatorFactory factory =
                new DoublesReservoirMergeAggregatorFactory("myFactory", 1024);
        final byte[] json = mapper.writeValueAsBytes(factory);
        final DoublesReservoirMergeAggregatorFactory fromJson =
                (DoublesReservoirMergeAggregatorFactory) mapper.readValue(json, AggregatorFactory.class);
        assertEquals(factory, fromJson);
    }

    @Test
    public void testWithName() {
        final DoublesReservoirMergeAggregatorFactory factory =
                new DoublesReservoirMergeAggregatorFactory("myFactory", 1024);
        assertEquals(factory, factory.withName("myFactory"));
        assertEquals("newTest", factory.withName("newTest").getName());
    }
}
