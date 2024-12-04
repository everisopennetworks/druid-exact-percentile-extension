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
package bi.deep.aggregation.percentiles.sql;

import bi.deep.aggregation.percentiles.aggregator.DoublesReservoirToPercentilesPostAggregator;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.PostAggregatorVisitor;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;

public class DoublesReservoirPercentilesOperatorConversion implements SqlOperatorConversion {
    private static final String NAME = "DR_GET_PERCENTILES";

    @Override
    public SqlOperator calciteOperator() {
        return new SqlFunction(
                NAME,
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.explicit(factory -> Calcites.createSqlType(factory, SqlTypeName.OTHER)),
                null,
                new ListArgOperandTypeChecker(),
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    @Nullable
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
        final PostAggregator aggregator = OperatorConversions.toPostAggregator(
                plannerContext, rowSignature, operands.get(0), postAggregatorVisitor, true);

        if (aggregator == null) {
            return null;
        }

        final double[] args = operands.subList(1, operands.size()).stream()
                .mapToDouble(operand -> ((Number) RexLiteral.value(operand)).doubleValue())
                .toArray();

        return new DoublesReservoirToPercentilesPostAggregator(
                postAggregatorVisitor.getOutputNamePrefix() + postAggregatorVisitor.getAndIncrementCounter(),
                aggregator,
                args);
    }
}
