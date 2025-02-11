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
package com.nttdata.druid;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Binder;
import com.nttdata.druid.aggregation.percentiles.aggregator.DoublesReservoirAggregatorFactory;
import com.nttdata.druid.aggregation.percentiles.aggregator.DoublesReservoirToPercentilePostAggregator;
import com.nttdata.druid.aggregation.percentiles.aggregator.DoublesReservoirToPercentilesPostAggregator;
import com.nttdata.druid.aggregation.percentiles.aggregator.DoublesReservoirToStddevPostAggregator;
import com.nttdata.druid.aggregation.percentiles.reservoir.DoublesReservoirComplexMetricSerde;
import com.nttdata.druid.aggregation.percentiles.sql.DoublesReservoirObjectSqlAggregator;
import com.nttdata.druid.aggregation.percentiles.sql.DoublesReservoirPercentileOperatorConversion;
import com.nttdata.druid.aggregation.percentiles.sql.DoublesReservoirPercentilesOperatorConversion;
import com.nttdata.druid.aggregation.percentiles.sql.DoublesReservoirStddevOperatorConversion;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.Collections;
import java.util.List;

public class DoublesReservoirModule implements DruidModule {
    public static final String TYPE_NAME = "doublesReservoir";
    public static final ColumnType TYPE = ColumnType.ofComplex(TYPE_NAME);

    @Override
    public void configure(Binder binder) {
        registerSerde();
        SqlBindings.addAggregator(binder, DoublesReservoirObjectSqlAggregator.class);

        SqlBindings.addOperatorConversion(binder, DoublesReservoirPercentileOperatorConversion.class);
        SqlBindings.addOperatorConversion(binder, DoublesReservoirPercentilesOperatorConversion.class);

        SqlBindings.addOperatorConversion(binder, DoublesReservoirStddevOperatorConversion.class);
    }

    @Override
    public List<? extends Module> getJacksonModules() {
        return Collections.singletonList(new SimpleModule(getClass().getSimpleName())
                .registerSubtypes(DoublesReservoirAggregatorFactory.class)
                .registerSubtypes(DoublesReservoirToPercentilePostAggregator.class)
                .registerSubtypes(DoublesReservoirToPercentilesPostAggregator.class)
                .registerSubtypes(DoublesReservoirToStddevPostAggregator.class)
        );
    }

    @VisibleForTesting
    public static void registerSerde() {
        ComplexMetrics.registerSerde(TYPE_NAME, new DoublesReservoirComplexMetricSerde());
    }
}
