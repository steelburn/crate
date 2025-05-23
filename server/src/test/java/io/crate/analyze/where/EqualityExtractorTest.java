/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.analyze.where;

import static io.crate.testing.Asserts.isLiteral;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.common.unit.TimeValue;
import io.crate.exceptions.JobKilledException;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.session.Session;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;

public class EqualityExtractorTest extends CrateDummyClusterServiceUnitTest {

    protected final CoordinatorTxnCtx coordinatorTxnCtx = new CoordinatorTxnCtx(CoordinatorSessionSettings.systemDefaults());
    private SqlExpressions expressions;
    protected EvaluatingNormalizer normalizer;
    private EqualityExtractor ee;

    private final ColumnIdent x = ColumnIdent.of("x");
    private final ColumnIdent i = ColumnIdent.of("i");

    @Before
    public void prepare() throws Exception {
        Map<RelationName, AnalyzedRelation> sources = T3.sources(List.of(T3.T1), clusterService);

        DocTableRelation tr1 = (DocTableRelation) sources.get(T3.T1);
        expressions = new SqlExpressions(sources, tr1);
        normalizer = EvaluatingNormalizer.functionOnlyNormalizer(expressions.nodeCtx);
        ee = new EqualityExtractor(normalizer);
    }

    private List<List<Symbol>> analyzeExact(Symbol query, List<ColumnIdent> primaryKeys) {
        return ee.extractMatches(primaryKeys, query, coordinatorTxnCtx, Session.TimeoutToken.noopToken()).matches();
    }

    private List<List<Symbol>> analyzeExact(EqualityExtractor ee, Symbol query, List<ColumnIdent> primaryKeys) {
        return ee.extractMatches(primaryKeys, query, coordinatorTxnCtx, Session.TimeoutToken.noopToken()).matches();
    }

    /**
     * Convert a query expression as text into a normalized query {@link Symbol}.
     * @param expression The query expression as text
     * @return the query expression as Symbol
     */
    private Symbol query(String expression) {
        return expressions.normalize(expressions.asSymbol(expression));
    }

    private Symbol query(SqlExpressions expressions, String expression) {
        return expressions.normalize(expressions.asSymbol(expression));
    }

    private List<List<Symbol>> analyzeParentX(Symbol query) {
        return ee.extractParentMatches(List.of(x), query, coordinatorTxnCtx, Session.TimeoutToken.noopToken()).matches();
    }

    private List<List<Symbol>> analyzeExactX(Symbol query) {
        return analyzeExact(query, List.of(x));
    }

    private List<List<Symbol>> analyzeExactXI(Symbol query) {
        return analyzeExact(query, List.of(x, i));
    }

    @Test
    public void testNoExtract2ColPKWithOr() throws Exception {
        Symbol query = query("x = 1 or i = 2");
        List<List<Symbol>> matches = analyzeExactXI(query);
        assertThat(matches).isNull();
    }

    @Test
    public void testNoExtractOnNotEqualsOnSinglePk() {
        Symbol query = query("x != 1");
        List<List<Symbol>> matches = analyzeExactX(query);
        assertThat(matches).isNull();
    }

    @Test
    public void testExtract2ColPKWithAndAndNestedOr() throws Exception {
        Symbol query = query("x = 1 and (i = 2 or i = 3 or i = 4)");
        List<List<Symbol>> matches = analyzeExactXI(query);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1), isLiteral(2)),
            s -> assertThat(s).satisfiesExactly(isLiteral(1), isLiteral(3)),
            s -> assertThat(s).satisfiesExactly(isLiteral(1), isLiteral(4))
        );
    }

    @Test
    public void testExtract2ColPKWithOrFullDistinctKeys() throws Exception {
        Symbol query = query("(x = 1 and i = 2) or (x = 3 and i =4)");
        List<List<Symbol>> matches = analyzeExactXI(query);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1), isLiteral(2)),
            s -> assertThat(s).satisfiesExactly(isLiteral(3), isLiteral(4))
        );
    }

    @Test
    public void testExtract2ColPKWithOrFullDuplicateKeys() throws Exception {
        Symbol query = query("(x = 1 and i = 2) or (x = 1 and i = 4)");
        List<List<Symbol>> matches = analyzeExactXI(query);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1), isLiteral(2)),
            s -> assertThat(s).satisfiesExactly(isLiteral(1), isLiteral(4))
        );
    }


    @Test
    public void testExtractRoutingFromAnd() throws Exception {
        Symbol query = query("x = 1 and i = 2");
        List<List<Symbol>> matches = analyzeParentX(query);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1)));
    }

    @Test
    public void testExtractNoRoutingFromForeignOnly() throws Exception {
        Symbol query = query("i = 2");
        List<List<Symbol>> matches = analyzeParentX(query);
        assertThat(matches).isNull();
    }

    @Test
    public void testExtractRoutingFromOr() throws Exception {
        Symbol query = query("x = 1 or x = 2");
        List<List<Symbol>> matches = analyzeParentX(query);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1)),
            s -> assertThat(s).satisfiesExactly(isLiteral(2)));
    }

    @Test
    public void testNoExtractSinglePKFromAnd() throws Exception {
        Symbol query = query("x = 1 and x = 2");
        List<List<Symbol>> matches = analyzeExactX(query);
        assertThat(matches).isNull();
    }


    @Test
    public void testExtractRoutingFromNestedOr() throws Exception {
        Symbol query = query("x =1 or x =2 or x = 3 or x = 4");
        List<List<Symbol>> matches = analyzeParentX(query);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1)),
            s -> assertThat(s).satisfiesExactly(isLiteral(2)),
            s -> assertThat(s).satisfiesExactly(isLiteral(3)),
            s -> assertThat(s).satisfiesExactly(isLiteral(4)));
    }

    @Test
    public void testExtractNoRoutingFromOrWithForeignColumn() throws Exception {
        Symbol query = query("x = 1 or i = 2");
        List<List<Symbol>> matches = analyzeParentX(query);
        assertThat(matches).isNull();
    }

    @Test
    public void testExtract2ColPKFromNestedOrWithDuplicates() throws Exception {
        Symbol query = query("x = 1 and (i = 2 or i = 2 or i = 4)");
        List<List<Symbol>> matches = analyzeExactXI(query);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1), isLiteral(2)),
            s -> assertThat(s).satisfiesExactly(isLiteral(1), isLiteral(4)));
    }


    @Test
    public void testNoExtract2ColPKFromAndEq1PartAnd2ForeignColumns() throws Exception {
        Symbol query = query("x = 1 and (i = 2 or a = 'a')");
        List<List<Symbol>> matches = analyzeExactXI(query);
        assertThat(matches).isNull();
    }


    /**
     * x=1 and (y=2 or ?)
     * and(x1, or(y2, ?)
     * <p>
     * x = 1 and (y=2 or x=3)
     * and(x1, or(y2, x3)
     * <p>
     * x=1 and (y=2 or y=3)
     * and(x1, or(or(y2, y3), y4))
     * <p>
     * branches: x1,
     * <p>
     * <p>
     * <p>
     * x=1 and (y=2 or F)
     * 1,2   1=1 and (2=2 or z=3) T
     */
    @Test
    public void testNoExtract2ColPKFromAndWithEq1PartAnd1ForeignColumnInOr() throws Exception {
        Symbol query = query("x = 1 and (i = 2 or a = 'a')");
        List<List<Symbol>> matches = analyzeExactXI(query);
        assertThat(matches).isNull();
    }

    @Test
    public void testExtract2ColPKFrom1PartAndOtherPart2EqOr() throws Exception {
        Symbol query = query("x = 1 and (i = 2 or i = 3)");
        List<List<Symbol>> matches = analyzeExactXI(query);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1), isLiteral(2)),
            s -> assertThat(s).satisfiesExactly(isLiteral(1), isLiteral(3)));
    }

    @Test
    public void testNoExtract2ColPKFromOnly1Part() throws Exception {
        Symbol query = query("x = 1");
        List<List<Symbol>> matches = analyzeExactXI(query);
        assertThat(matches).isNull();
    }

    @Test
    public void testExtractSinglePKFromAnyEq() throws Exception {
        Symbol query = query("x = any([1, 2, 3])");
        List<List<Symbol>> matches = analyzeExactX(query);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1)),
            s -> assertThat(s).satisfiesExactly(isLiteral(2)),
            s -> assertThat(s).satisfiesExactly(isLiteral(3)));
    }

    @Test
    public void testExtract2ColPKFromAnyEq() throws Exception {
        Symbol query = query("i = 4 and x = any([1, 2, 3])");
        List<List<Symbol>> matches = analyzeExactXI(query);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1), isLiteral(4)),
            s -> assertThat(s).satisfiesExactly(isLiteral(2), isLiteral(4)),
            s -> assertThat(s).satisfiesExactly(isLiteral(3), isLiteral(4)));
    }

    @Test
    public void testExtractSinglePKFromAnyEqInOr() throws Exception {
        Symbol query = query("x = any([1, 2, 3]) or x = any([4, 5, 3])");
        List<List<Symbol>> matches = analyzeExactX(query);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1)),
            s -> assertThat(s).satisfiesExactly(isLiteral(2)),
            s -> assertThat(s).satisfiesExactly(isLiteral(3)),
            s -> assertThat(s).satisfiesExactly(isLiteral(4)),
            s -> assertThat(s).satisfiesExactly(isLiteral(5)));
    }

    @Test
    public void testExtractSinglePKFromOrInAnd() throws Exception {
        Symbol query = query("(x = 1 or x = 2 or x = 3) and (x = 1 or x = 4 or x = 5)");
        List<List<Symbol>> matches = analyzeExactX(query);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1)));
    }

    @Test
    public void testExtractSinglePK1FromAndAnyEq() throws Exception {
        Symbol query = query("x = any([1, 2, 3]) and x = any([4, 5, 3])");
        List<List<Symbol>> matches = analyzeExactX(query);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(3)));
    }

    @Test
    public void testExtract2ColPKFromAnyEqAnd() throws Exception {
        Symbol query = query("x = any([1, 2, 3]) and i = any([1, 2, 3])");
        List<List<Symbol>> matches = analyzeExactXI(query);
        // cartesian product: 3 * 3
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1), isLiteral(1)),
            s -> assertThat(s).satisfiesExactly(isLiteral(1), isLiteral(2)),
            s -> assertThat(s).satisfiesExactly(isLiteral(1), isLiteral(3)),
            s -> assertThat(s).satisfiesExactly(isLiteral(2), isLiteral(1)),
            s -> assertThat(s).satisfiesExactly(isLiteral(2), isLiteral(2)),
            s -> assertThat(s).satisfiesExactly(isLiteral(2), isLiteral(3)),
            s -> assertThat(s).satisfiesExactly(isLiteral(3), isLiteral(1)),
            s -> assertThat(s).satisfiesExactly(isLiteral(3), isLiteral(2)),
            s -> assertThat(s).satisfiesExactly(isLiteral(3), isLiteral(3)));
    }

    @Test
    public void testNoPKExtractionIfMatchIsPresent() throws Exception {
        Symbol query = query("x in (1, 2, 3) and match(a, 'Hello World')");
        List<List<Symbol>> matches = analyzeExactX(query);
        assertThat(matches).isNull();
    }

    @Test
    public void testNoPKExtractionOnNotIn() {
        List<List<Symbol>> matches = analyzeExactX(query("x not in (1, 2, 3)"));
        assertThat(matches).isNull();
    }

    @Test
    public void testNoPKExtractionWhenColumnsOnBothSidesOfEqual() {
        List<List<Symbol>> matches = analyzeExactX(query("x = abs(x)"));
        assertThat(matches).isNull();
    }

    @Test
    public void test_primary_key_comparison_is_not_detected_inside_cast_function() throws Exception {
        Symbol query = query("cast(x as bigint) = 0");
        List<List<Symbol>> matches = analyzeExactX(query);
        assertThat(matches).isNull();

        query = query("cast(x as bigint) = any([1, 2, 3])");
        matches = analyzeExactX(query);
        assertThat(matches).isNull();
    }

    @Test
    public void test_primary_key_extraction_on_subscript_with_any() {
        Map<RelationName, AnalyzedRelation> sources = T3.sources(List.of(T3.T4), clusterService);
        DocTableRelation tr4 = (DocTableRelation) sources.get(T3.T4);
        var expressionsT4 = new SqlExpressions(sources, tr4);
        var pkCol = ColumnIdent.of("obj");

        var query = expressionsT4.normalize(expressionsT4.asSymbol("obj = any([{i = 1}])"));
        List<List<Symbol>> matches = analyzeExact(query, List.of(pkCol));
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(Map.of("i", 1))));
    }

    @Test
    public void test_primary_key_extraction_if_combined_with_and_operator() throws Exception {
        Symbol query = query("x = 1 and a = 'foo' or (x = 3 and a = 'bar')");
        List<List<Symbol>> matches = analyzeExactX(query);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1)),
            s -> assertThat(s).satisfiesExactly(isLiteral(3)));
    }

    @Test
    public void test_primary_key_extraction_if_combined_with_and_scalar() throws Exception {
        Symbol query = query("x in (1, 2, 3) and substr(cast(x as string), 0) = 4");
        List<List<Symbol>> matches = analyzeExactX(query);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1)),
            s -> assertThat(s).satisfiesExactly(isLiteral(2)),
            s -> assertThat(s).satisfiesExactly(isLiteral(3)));
    }

    // tracks a bug: https://github.com/crate/crate/issues/15458
    @Test
    public void test_no_pk_extraction_from_pk_eq_pk() {
        List<List<Symbol>> matches = analyzeExactX(query("NOT((i NOT IN (1)) AND (x != 2))"));
        assertThat(matches).isNull();
        matches = analyzeExactX(query("(i IN (1)) OR (x = 2)")); // equivalent to above
        assertThat(matches).isNull();
    }

    // tracks a bug: https://github.com/crate/crate/issues/15395
    @Test
    public void test_no_pk_extraction_if_the_pk_is_under_is_null() {
        List<List<Symbol>> matches = analyzeExactX(query("NOT(x != 1 AND x IS NULL)"));
        assertThat(matches).isNull();
        matches = analyzeExactX(query("(x = 1) OR (x IS NOT NULL)")); // equivalent to above
        assertThat(matches).isNull();
    }

    @Test
    public void test_no_pk_extraction_if_the_pk_is_under_not() {
        List<List<Symbol>> matches = analyzeExactX(query("x != 1 or x = 1"));
        assertThat(matches).isNull();
        matches = analyzeExactX(query("not(x != 1) or x = 1"));
        assertThat(matches).isNull();
        matches = analyzeExactX(query("not(i != 1 and x = 1)"));
        assertThat(matches).isNull();
        matches = analyzeExactX(query("x = 1 or (x = 2 or (x = 3 or not(x = 4)))"));
        assertThat(matches).isNull();
    }

    @Test
    public void test_pk_extraction_if_another_col_is_under_not() {
        List<List<Symbol>> matches = analyzeExactX(query("not(i = 1) and x = 1"));
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1)));
    }

    // tracks a bug: https://github.com/crate/crate/issues/15592
    @Test
    public void test_no_pk_extraction_if_nonPK_column_under_or() {
        List<List<Symbol>> matches = analyzeExactX(query("x = 1 OR i = 1"));
        assertThat(matches).isNull();
        matches = analyzeExactX(query("x = 1 OR NOT i = 1"));
        assertThat(matches).isNull();
        matches = analyzeExactX(query("x = 1 AND (x = 2 OR i = 1)"));
        assertThat(matches).isNull();
        matches = analyzeExactX(query("x = 1 AND (x = 2 AND (NOT(x = 3) OR i = 1) AND x = 4)"));
        assertThat(matches).isNull();
    }

    @Test
    public void test_pk_extraction_breaks_after_x_iterations() {
        StringJoiner sj = new StringJoiner(" or ");
        for (int j = 0; j < 20; j++) {
            sj.add("x = ? AND i = ?");
        }

        EqualityExtractor extractor = new EqualityExtractor(normalizer) {
            @Override
            protected int maxIterations() {
                return 100; // with 20 `x=? AND i=?` joined with OR, 100 iterations are not enough
            }
        };

        var matches = extractor.extractMatches(
            List.of(x, i), query(sj.toString()), coordinatorTxnCtx, Session.TimeoutToken.noopToken()).matches();
        assertThat(matches).isNull();

        extractor = new EqualityExtractor(normalizer) {
            @Override
            protected int maxIterations() {
                return 1000; // make sure iterations are enough to extract pk matches
            }
        };
        matches = extractor.extractMatches(
            List.of(x, i), query(sj.toString()), coordinatorTxnCtx, Session.TimeoutToken.noopToken()).matches();
        assertThat(matches).isNotNull();
    }

    @Test
    public void test_pk_extraction_interrupted_when_exceeds_timeout() {
        Session.TimeoutToken token = new TestToken(TimeValue.timeValueMillis(10), 3);
        StringJoiner sj = new StringJoiner(" or ");
        for (int j = 0; j < 20; j++) {
            sj.add("x = ? AND i = ?");
        }

        EqualityExtractor extractor = new EqualityExtractor(normalizer) {
            @Override
            protected int maxIterations() {
                return 1000;
            }
        };

        assertThatThrownBy(() -> extractor.extractMatches(
            List.of(x, i),
            query(sj.toString()),
            coordinatorTxnCtx,
            token).matches()
        )
            .isExactlyInstanceOf(JobKilledException.class)
            .hasMessage("Job killed. statement_timeout (10ms)");
    }

    @Test
    public void test_no_exact_result_on_partial_match() throws Exception {
        // https://github.com/crate/crate/issues/17197
        // It is important that the query first hits a non-pk column
        Symbol query = query("(i = 1 or x = 2) and (i = 2 and a = 'foo')");
        List<List<Symbol>> analyzeExact = analyzeExact(query, List.of(ColumnIdent.of("x")));
        assertThat(analyzeExact).isNull();
    }

    @Test
    public void test_standalone_boolean_column_transformed_to_eq_true_expression() throws Exception {
        // Not using an existing table definition because adding a boolean column affects other unrelated tests.
        var relationName = new RelationName(DocSchemaInfo.NAME, "test");
        DocTableInfo tableInfo = SQLExecutor.tableInfo(
            relationName,
            "create table test(b boolean, primary key(b))",
            clusterService
        );
        TableRelation tableRelation = new TableRelation(tableInfo);
        Map<RelationName, AnalyzedRelation> tableSources = Map.of(tableInfo.ident(), tableRelation);
        SqlExpressions expressions = new SqlExpressions(tableSources, tableRelation);
        EvaluatingNormalizer normalizer = EvaluatingNormalizer.functionOnlyNormalizer(expressions.nodeCtx);
        EqualityExtractor ee = new EqualityExtractor(normalizer);


        List<List<Symbol>> matches = analyzeExact(ee, query(expressions, "b or b = false"), List.of(ColumnIdent.of("b")));
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(true)),
            s -> assertThat(s).satisfiesExactly(isLiteral(false))
        );

        // No duplicate comparisons, true appears only once
        matches = analyzeExact(ee, query(expressions, "b or b or b = false"), List.of(ColumnIdent.of("b")));
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(true)),
            s -> assertThat(s).satisfiesExactly(isLiteral(false))
        );

        matches = analyzeExact(ee, query(expressions, "b or (b in (null))"), List.of(ColumnIdent.of("b")));
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(true)),
            s -> assertThat(s).satisfiesExactly(isLiteral(null))
        );
    }


    public static class TestToken extends Session.TimeoutToken {

        private final int maxChecks;
        private int checks;

        public TestToken(TimeValue statementTimeout, int maxChecks) {
            super(statementTimeout, System.nanoTime());
            this.checks = 0;
            this.maxChecks = maxChecks;
        }

        public void check() {
            checks++;
            if (statementTimeout.nanos() > 0 && checks > maxChecks) {
                throw JobKilledException.of("statement_timeout (" + statementTimeout + ")");
            }
        }
    }

}
