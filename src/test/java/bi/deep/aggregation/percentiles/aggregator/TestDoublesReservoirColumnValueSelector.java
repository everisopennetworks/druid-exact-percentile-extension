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

import bi.deep.aggregation.percentiles.reservoir.DoublesReservoir;
import javax.annotation.Nullable;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;

class TestDoublesReservoirColumnValueSelector implements ColumnValueSelector<DoublesReservoir> {
    @Override
    public double getDouble() {
        return 99;
    }

    @Override
    public float getFloat() {
        return 99;
    }

    @Override
    public long getLong() {
        return 99;
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Nullable
    @Override
    public DoublesReservoir getObject() {
        return DoublesReservoir.EMPTY;
    }

    @Override
    public Class<? extends DoublesReservoir> classOfObject() {
        return DoublesReservoir.class;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector) {
        // No-op
    }
}
