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
package bi.deep;

import bi.deep.aggregation.percentiles.aggregator.DoublesReservoirAggregatorFactory;
import bi.deep.aggregation.percentiles.aggregator.DoublesReservoirMergeAggregatorFactory;
import bi.deep.aggregation.percentiles.aggregator.DoublesReservoirToPercentilePostAggregator;
import bi.deep.aggregation.percentiles.aggregator.DoublesReservoirToPercentilesPostAggregator;
import bi.deep.aggregation.percentiles.sql.DoublesReservoirObjectSqlAggregator;
import bi.deep.aggregation.percentiles.sql.DoublesReservoirPercentileOperatorConversion;
import bi.deep.aggregation.percentiles.sql.DoublesReservoirPercentilesOperatorConversion;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import java.util.Collections;
import java.util.List;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.sql.guice.SqlBindings;

public class DoublesReservoirModule implements DruidModule {

    @Override
    public void configure(Binder binder) {
        SqlBindings.addAggregator(binder, DoublesReservoirObjectSqlAggregator.class);

        SqlBindings.addOperatorConversion(binder, DoublesReservoirPercentileOperatorConversion.class);
        SqlBindings.addOperatorConversion(binder, DoublesReservoirPercentilesOperatorConversion.class);
    }

    @Override
    public List<? extends Module> getJacksonModules() {
        return Collections.singletonList(new SimpleModule(getClass().getSimpleName())
                .registerSubtypes(DoublesReservoirAggregatorFactory.class)
                .registerSubtypes(DoublesReservoirMergeAggregatorFactory.class)
                .registerSubtypes(DoublesReservoirToPercentilePostAggregator.class)
                .registerSubtypes(DoublesReservoirToPercentilesPostAggregator.class));
    }
}
