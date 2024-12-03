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

import bi.deep.DoublesReservoirModule;
import java.nio.ByteBuffer;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexColumnPartSupplier;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;

public class DoublesReservoirComplexMetricSerde extends ComplexMetricSerde {
    private static final DoublesReservoirObjectStrategy STRATEGY = new DoublesReservoirObjectStrategy();

    @Override
    public String getTypeName() {
        return DoublesReservoirModule.TYPE_NAME;
    }

    @Override
    public ComplexMetricExtractor getExtractor() {
        return new DoublesReservoirComplexMetricExtractor();
    }

    @Override
    public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder) {
        final GenericIndexed<DoublesReservoir> ge =
                GenericIndexed.read(buffer, getObjectStrategy(), builder.getFileMapper());
        builder.setComplexColumnSupplier(new ComplexColumnPartSupplier(getTypeName(), ge));
    }

    @Override
    @SuppressWarnings("deprecated")
    public ObjectStrategy<DoublesReservoir> getObjectStrategy() {
        return STRATEGY;
    }
}
