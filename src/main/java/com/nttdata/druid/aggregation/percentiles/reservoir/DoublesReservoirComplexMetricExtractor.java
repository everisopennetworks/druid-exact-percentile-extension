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

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.segment.serde.ComplexMetricExtractor;

import javax.annotation.Nullable;

public class DoublesReservoirComplexMetricExtractor implements ComplexMetricExtractor {

    @Override
    public Class extractedClass() {
        return DoublesReservoir.class;
    }

    @Nullable
    @Override
    public Object extractValue(InputRow inputRow, String metricName) {
        final Object object = inputRow.getRaw(metricName);

        if (object == null || object instanceof Number || object instanceof DoublesReservoir) {
            return object;
        }

        if (object instanceof String && NumberUtils.isCreatable((String) object)) {
            return Double.parseDouble((String) object);
        }

        return DoublesReservoir.deserialize(object);
    }
}
