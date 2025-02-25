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

import com.nttdata.druid.aggregation.percentiles.aggregator.DoublesReservoirToPercentilePostAggregator;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.PostAggregatorVisitor;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.List;

public class DoublesReservoirPercentileOperatorConversion implements SqlOperatorConversion {
    private static final String NAME = "DR_GET_PERCENTILE";
    private static final SqlFunction SQL_FUNCTION = OperatorConversions.operatorBuilder(StringUtils.toUpperCase(NAME))
            .operandTypes(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC)
            .returnTypeNonNull(SqlTypeName.DOUBLE)
            .build();

    @Override
    public SqlOperator calciteOperator() {
        return SQL_FUNCTION;
    }

    @Override
    public DruidExpression toDruidExpression(
            PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode) {
        return null;
    }

    @Nullable
    @Override
    public PostAggregator toPostAggregator(
            PlannerContext plannerContext,
            RowSignature rowSignature,
            RexNode rexNode,
            PostAggregatorVisitor postAggregatorVisitor) {
        final List<RexNode> operands = ((RexCall) rexNode).getOperands();
        final PostAggregator firstOperand = OperatorConversions.toPostAggregator(
                plannerContext, rowSignature, operands.get(0), postAggregatorVisitor, true);

        if (firstOperand == null) {
            return null;
        }

        if (!operands.get(1).isA(SqlKind.LITERAL)) {
            return null;
        }

        final double arg = ((Number) RexLiteral.value(operands.get(1))).doubleValue();

        return new DoublesReservoirToPercentilePostAggregator(
                postAggregatorVisitor.getOutputNamePrefix() + postAggregatorVisitor.getAndIncrementCounter(),
                firstOperand,
                arg);
    }
}
