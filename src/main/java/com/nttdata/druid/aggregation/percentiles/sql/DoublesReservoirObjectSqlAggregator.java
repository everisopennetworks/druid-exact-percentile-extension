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
package com.nttdata.druid.aggregation.percentiles.sql;

import com.google.common.collect.ImmutableList;
import com.nttdata.druid.aggregation.percentiles.aggregator.DoublesReservoirAggregatorFactory;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.Aggregations;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.InputAccessor;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.List;

import static com.nttdata.druid.DoublesReservoirModule.TYPE;

public class DoublesReservoirObjectSqlAggregator implements SqlAggregator {
    private static final String NAME = "DR_PERCENTILE_AGG";
    private static final SqlAggFunction FUNCTION_INSTANCE = OperatorConversions.aggregatorBuilder(NAME)
            .operandNames("column", "maxSize")
            .operandTypes(SqlTypeFamily.ANY, SqlTypeFamily.EXACT_NUMERIC)
            .returnTypeInference(Calcites.complexReturnTypeWithNullability(TYPE, false))
            .requiredOperandCount(2)
            .literalOperands(1)
            .functionCategory(SqlFunctionCategory.NUMERIC)
            .build();

    @Override
    public SqlAggFunction calciteFunction() {
        return FUNCTION_INSTANCE;
    }

    @Nullable
    @Override
    public Aggregation toDruidAggregation(
            final PlannerContext plannerContext,
            final VirtualColumnRegistry virtualColumnRegistry,
            final String name,
            final AggregateCall aggregateCall,
            final InputAccessor inputAccessor,
            final List<Aggregation> existingAggregations,
            final boolean finalizeAggregations) {
        final DruidExpression input = Aggregations.toDruidExpressionForNumericAggregator(
                plannerContext,
                inputAccessor.getInputRowSignature(),
                inputAccessor.getField(aggregateCall.getArgList().get(0)));
        if (input == null) {
            return null;
        }

        final RexNode resolutionArg =
                inputAccessor.getField(aggregateCall.getArgList().get(1));

        if (!resolutionArg.isA(SqlKind.LITERAL)) {
            // Resolution must be a literal in order to plan.
            return null;
        }

        final String fieldName = StringUtils.format("%s:agg", name);
        final int maxSize = ((Number) RexLiteral.value(resolutionArg)).intValue();
        final AggregatorFactory aggregatorFactory;

        // No existing match found. Create a new one.
        if (input.isDirectColumnAccess()) {
            aggregatorFactory = new DoublesReservoirAggregatorFactory(fieldName, input.getDirectColumn(), maxSize);
        } else {
            String virtualColumnName =
                    virtualColumnRegistry.getOrCreateVirtualColumnForExpression(input, ColumnType.FLOAT);
            aggregatorFactory = new DoublesReservoirAggregatorFactory(fieldName, virtualColumnName, maxSize);
        }

        return Aggregation.create(ImmutableList.of(aggregatorFactory), null);
    }
}
