/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.execution.engine.aggregation.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.Test;

import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Scalar;
import io.crate.metadata.SearchPath;
import io.crate.metadata.functions.Signature;
import io.crate.operation.aggregation.AggregationTestCase;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class VarianceAggregationTest extends AggregationTestCase {

    private Object executeAggregation(DataType<?> argumentType, Object[][] data) throws Exception {
        return executeAggregation(
                Signature.builder(VarianceAggregation.NAME, FunctionType.AGGREGATE)
                        .argumentTypes(argumentType.getTypeSignature())
                        .returnType(DataTypes.DOUBLE.getTypeSignature())
                        .features(Scalar.Feature.DETERMINISTIC)
                        .build(),
                data,
                List.of()
        );
    }

    @Test
    public void test_function_implements_doc_values_aggregator_for_numeric_types() {
        for (var dataType : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            assertHasDocValueAggregator(VarianceAggregation.NAME, List.of(dataType));
        }
    }

    @Test
    public void testReturnType() throws Exception {
        for (var dataType : VarianceAggregation.SUPPORTED_TYPES) {
            // Return type is fixed to Double
            var varianceFunction = nodeCtx.functions().get(
                null,
                VarianceAggregation.NAME,
                List.of(Literal.of(dataType, null)),
                SearchPath.pathWithPGCatalogAndDoc()
            );
            assertThat(varianceFunction.boundSignature().returnType()).isEqualTo(DataTypes.DOUBLE);
        }
    }

    @Test
    public void withNullArg() throws Exception {
        Object result = executeAggregation(DataTypes.DOUBLE, new Object[][]{{null}, {null}});
        assertThat(result).isNull();
    }

    @Test
    public void testDouble() throws Exception {
        Object result = executeAggregation(DataTypes.DOUBLE, new Object[][]{{1.0d}, {1.0d}, {1.0d}, {null}});

        assertThat(result).isEqualTo(0.0d);
    }

    @Test
    public void testFloat() throws Exception {
        Object result = executeAggregation(DataTypes.FLOAT, new Object[][]{{0.7f}, {0.3f}, {0.7f}});

        assertThat(result).isEqualTo(0.035555551317003165d);
    }

    @Test
    public void testInteger() throws Exception {
        Object result = executeAggregation(DataTypes.INTEGER, new Object[][]{{7}, {3}});

        assertThat(result).isEqualTo(4d);
    }

    @Test
    public void testLong() throws Exception {
        Object result = executeAggregation(DataTypes.LONG, new Object[][]{{7L}, {3L}});

        assertThat(result).isEqualTo(4d);
    }

    @Test
    public void testShort() throws Exception {
        Object result = executeAggregation(DataTypes.SHORT, new Object[][]{{(short) 7}, {(short) 3}});

        assertThat(result).isEqualTo(4d);
    }

    @Test
    public void test_variance_with_byte_argument_type() throws Exception {
        Object result = executeAggregation(DataTypes.BYTE, new Object[][]{{(byte) 1}, {(byte) 1}});

        assertThat(result).isEqualTo(0d);
    }

    @Test
    public void testUnsupportedType() throws Exception {
        assertThatThrownBy(() -> executeAggregation(DataTypes.GEO_POINT, new Object[][] {}))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessage("Invalid arguments in: variance(INPUT(0)) with (geo_point). Valid types: (double precision), (real), (byte), (smallint), (integer), (bigint), (timestamp with time zone)");
    }
}
