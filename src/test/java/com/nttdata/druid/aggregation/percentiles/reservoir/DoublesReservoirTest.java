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
package com.nttdata.druid.aggregation.percentiles.reservoir;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.druid.java.util.common.IAE;
import org.junit.jupiter.api.Test;

class DoublesReservoirTest {
    @Test
    void testExactPercentiles() {
        List<Double> data = ImmutableList.of(0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0);
        DoublesReservoir reservoir = new DoublesReservoir(data.size(), data);

        // Percentiles with exact ranks
        assertEquals(0.0, reservoir.getPercentile(0.0));
        assertEquals(1.0, reservoir.getPercentile(0.1));
        assertEquals(2.0, reservoir.getPercentile(0.2));
        assertEquals(3.0, reservoir.getPercentile(0.3));
        assertEquals(4.0, reservoir.getPercentile(0.4));
        assertEquals(5.0, reservoir.getPercentile(0.5));
        assertEquals(6.0, reservoir.getPercentile(0.6));
        assertEquals(7.0, reservoir.getPercentile(0.7));
        assertEquals(8.0, reservoir.getPercentile(0.8));
        assertEquals(9.0, reservoir.getPercentile(0.9));
        assertEquals(10.0, reservoir.getPercentile(1.0));
    }

    @Test
    void testExactPercentilesWithMultipleFractions() {
        List<Double> data = ImmutableList.of(0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0);
        DoublesReservoir reservoir = new DoublesReservoir(data.size(), data);
        double[] fractions = data.stream()
                .mapToDouble(Double::doubleValue)
                .map(d -> d / 10.0)
                .toArray();
        List<Double> result =
                Arrays.stream(reservoir.getPercentile(fractions)).boxed().collect(Collectors.toList());
        assertEquals(data, result);
    }

    @Test
    public void testPercentileInterpolation() {
        List<Double> list = ImmutableList.of(0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0);
        DoublesReservoir reservoir = new DoublesReservoir(list.size(), list);

        // Interpolated values
        assertEquals(0.5, reservoir.getPercentile(0.05), 0.01);
        assertEquals(1.5, reservoir.getPercentile(0.15), 0.01);
        assertEquals(2.5, reservoir.getPercentile(0.25), 0.01);
        assertEquals(3.5, reservoir.getPercentile(0.35), 0.01);
        assertEquals(4.5, reservoir.getPercentile(0.45), 0.01);
        assertEquals(5.5, reservoir.getPercentile(0.55), 0.01);
        assertEquals(6.5, reservoir.getPercentile(0.65), 0.01);
        assertEquals(7.5, reservoir.getPercentile(0.75), 0.01);
        assertEquals(8.5, reservoir.getPercentile(0.85), 0.01);
        assertEquals(9.5, reservoir.getPercentile(0.95), 0.01);
    }

    @Test
    public void testPercentileInterpolationWithMultipleFraction() {
        List<Double> list = ImmutableList.of(0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0);
        DoublesReservoir reservoir = new DoublesReservoir(list.size(), list);

        double[] expected = new double[] {0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5};
        double[] actual =
                reservoir.getPercentile(new double[] {0.05, 0.15, 0.25, 0.35, 0.45, 0.55, 0.65, 0.75, 0.85, 0.95});

        assertArrayEquals(expected, actual);
    }

    @Test
    public void testEmptyList() {
        DoublesReservoir reservoir = new DoublesReservoir(10);
        assertEquals(Double.NaN, reservoir.getPercentile(0.5));
    }

    @Test
    public void testInvalidFraction() {
        DoublesReservoir reservoir = new DoublesReservoir(10);

        IAE iae = assertThrows(IAE.class, () -> reservoir.getPercentile(1.1));
        assertEquals("A fraction must be >= 0 and <= 1.0: 1.1", iae.getMessage());

        iae = assertThrows(IAE.class, () -> reservoir.getPercentile(-0.1));
        assertEquals("A fraction must be >= 0 and <= 1.0: -0.1", iae.getMessage());
    }
}
