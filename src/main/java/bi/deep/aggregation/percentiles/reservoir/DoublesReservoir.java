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
package bi.deep.aggregation.percentiles.reservoir;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import org.apache.commons.collections.CollectionUtils;

@JsonSerialize(using = DoublesReservoirSerializer.class)
@JsonDeserialize(using = DoublesReservoirDeserializer.class)
public class DoublesReservoir implements Serializable {
    public static final Comparator<DoublesReservoir> COMPARATOR =
            Comparator.nullsFirst(Comparator.comparingInt(DoublesReservoir::hashCode));

    public static final DoublesReservoir EMPTY = new DoublesReservoir(0, Collections.emptyList(), true);
    private static final Random RANDOM = new Random();

    private final int maxSize;
    private int totalItemsSeen;
    private final List<Double> reservoir;
    private boolean alreadySorted;

    public DoublesReservoir(int maxSize) {
        this(maxSize, new ArrayList<>(maxSize), false);
    }

    public DoublesReservoir(int maxSize, List<Double> reservoir, boolean alreadySorted) {
        this(maxSize, reservoir, alreadySorted, reservoir.size());
    }

    public DoublesReservoir(int maxSize, List<Double> reservoir, boolean alreadySorted, int totalItemsSeen) {
        this.maxSize = maxSize;
        this.reservoir = reservoir;
        this.totalItemsSeen = totalItemsSeen;
        this.alreadySorted = alreadySorted;
    }

    public void addAll(List<Double> values) {
        if (CollectionUtils.isNotEmpty(values)) {
            values.forEach(this::add);
        }
    }

    public void add(double value) {
        ++totalItemsSeen;
        alreadySorted = false; // reset

        if (reservoir.size() < maxSize) {
            reservoir.add(value);
        } else {
            int index = RANDOM.nextInt(totalItemsSeen);
            if (index < maxSize) {
                reservoir.set(index, value);
            }
        }
    }

    public List<Double> getSortedValues() {
        final List<Double> sorted = new ArrayList<>(reservoir);

        if (!alreadySorted) {
            Collections.sort(sorted);
        }

        return sorted;
    }

    public double getPercentile(double fraction) {
        return calculate(getSortedValues(), fraction);
    }

    public double[] getPercentile(double[] fraction) {
        final double[] percentiles = new double[fraction.length];
        final List<Double> sorted = getSortedValues();

        for (int index = 0; index < fraction.length; ++index) {
            percentiles[index] = calculate(sorted, fraction[index]);
        }

        return percentiles;
    }

    private static double calculate(List<Double> sortedList, double fraction) {
        if (sortedList.isEmpty()) {
            return Double.NaN;
        }

        int index = (int) Math.ceil(fraction * sortedList.size()) - 1;
        return sortedList.get(Math.max(0, Math.min(index, sortedList.size() - 1)));
    }

    public DoublesReservoir merge(DoublesReservoir source) {
        if (source != null) {
            this.addAll(source.reservoir);
        }

        return this;
    }

    public List<Double> get() {
        return getSortedValues();
    }

    public static DoublesReservoir from(List<Double> value) {
        return new DoublesReservoir(value.size(), value, true);
    }

    public static DoublesReservoir deserialize(Object serializedReservoir, int maxSize) {
        if (serializedReservoir == null) {
            return null;
        }

        if (serializedReservoir instanceof DoublesReservoir) {
            return (DoublesReservoir) serializedReservoir;
        }

        if (serializedReservoir instanceof List) {
            @SuppressWarnings("unchecked")
            final DoublesReservoir result = new DoublesReservoir(maxSize, (List<Double>) serializedReservoir, true);
            return result;
        }

        throw new IllegalArgumentException("Cannot deserialize object of type "
                + serializedReservoir.getClass().getName());
    }
}
