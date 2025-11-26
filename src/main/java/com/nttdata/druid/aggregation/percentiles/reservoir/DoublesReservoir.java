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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.druid.java.util.common.IAE;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;

public class DoublesReservoir implements Serializable {
    public static final Comparator<DoublesReservoir> COMPARATOR =
            Comparator.nullsFirst(Comparator.comparingInt(DoublesReservoir::hashCode));

    public static final DoublesReservoir EMPTY = new DoublesReservoir(0, Collections.emptyList());
    private static final Random RANDOM = new Random();

    private final int maxSize;
    private int totalItemsSeen;
    private final List<Double> reservoir;
    private boolean alreadySorted = true;

    public DoublesReservoir(int maxSize) {
        this(maxSize, new ArrayList<>(maxSize));
    }

    public DoublesReservoir(int maxSize, List<Double> reservoir) {
        this(maxSize, reservoir, reservoir.size());
    }

    @JsonCreator
    public DoublesReservoir(
            @JsonProperty("maxSize") int maxSize,
            @JsonProperty("reservoir") List<Double> reservoir,
            @JsonProperty("totalItemsSeen") int totalItemsSeen) {
        this.maxSize = maxSize;
        this.reservoir = reservoir;
        this.totalItemsSeen = totalItemsSeen;
    }

    public void addAll(List<Double> values) {
        if (CollectionUtils.isNotEmpty(values)) {
            values.forEach(this::accept);
        }
    }

    public void accept(double value) {
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

    @JsonProperty
    public int getTotalItemsSeen() {
        return totalItemsSeen;
    }

    @JsonProperty
    public int getMaxSize() {
        return maxSize;
    }

    @JsonProperty("reservoir")
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

    @JsonIgnore
    public double getStddev() {
        return new StandardDeviation().evaluate(reservoir.stream().mapToDouble(Double::doubleValue).toArray());
    }

    @JsonIgnore
    public double getAVG() {
        return new Mean().evaluate(reservoir.stream().mapToDouble(Double::doubleValue).toArray());
    }

    private static void checkFractionBound(final double fraction) {
        if ((fraction < 0.0) || (fraction > 1.0)) {
            throw new IAE("A fraction must be >= 0 and <= 1.0: " + fraction);
        }
    }

    private static double calculate(List<Double> sortedList, double fraction) {
        checkFractionBound(fraction);

        if (sortedList.isEmpty()) {
            return Double.NaN;
        }

        double rank = fraction * (sortedList.size() - 1);
        int lowerIndex = (int) Math.floor(rank);
        int upperIndex = (int) Math.ceil(rank);

        if (lowerIndex == upperIndex) {
            return sortedList.get(lowerIndex);
        }

        double weight = rank - lowerIndex;

        // Interpolate between the two values
        return sortedList.get(lowerIndex) + weight * (sortedList.get(upperIndex) - sortedList.get(lowerIndex));
    }

    public DoublesReservoir mergeWith(@Nullable DoublesReservoir source) {
        if (source != null) {
            this.addAll(source.reservoir);
        }

        return this;
    }

    public static DoublesReservoir from(List<Double> value) {
        return new DoublesReservoir(value.size(), value);
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public static DoublesReservoir deserialize(@Nullable Object data) {
        if (data == null) {
            return DoublesReservoir.EMPTY;
        }

        if (data instanceof Map) {
            return DoublesReservoirUtils.convert(data);
        }

        if (data instanceof DoublesReservoir) {
            return (DoublesReservoir) data;
        }

        if (data instanceof String) {
            final String json = (String) data;

            if (StringUtils.isEmpty(json)) {
                return null;
            }

            try {
                return DoublesReservoirUtils.readJson(json);
            } catch (JsonProcessingException e) {
                throw new IAE(
                        "Cannot deserialize object of type " + data.getClass().getName());
            }
        }

        if (data instanceof List) {
            return DoublesReservoir.from((List<Double>) data);
        }

        throw new IAE("Cannot deserialize object of type " + data.getClass().getName());
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{"
                + "maxSize=" + maxSize
                + ", alreadySorted=" + alreadySorted
                + ", totalItemsSeen=" + totalItemsSeen
                + ", reservoir=" + reservoir
                + "}";
    }
}
