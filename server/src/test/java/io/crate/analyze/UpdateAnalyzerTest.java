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

package io.crate.analyze;

import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.Asserts.exactlyInstanceOf;
import static io.crate.testing.Asserts.isAlias;
import static io.crate.testing.Asserts.isFunction;
import static io.crate.testing.Asserts.isLiteral;
import static io.crate.testing.Asserts.isReference;
import static io.crate.testing.Asserts.toCondition;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.relations.DocTableRelation;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.ColumnValidationException;
import io.crate.exceptions.ConversionException;
import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.exceptions.VersioningValidationException;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.LtOperator;
import io.crate.expression.predicate.NotPredicate;
import io.crate.expression.scalar.CurrentDateFunction;
import io.crate.expression.scalar.cast.ImplicitCastFunction;
import io.crate.expression.symbol.Assignments;
import io.crate.expression.symbol.DynamicReference;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.ParameterSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.DoubleType;
import io.crate.types.ObjectType;

public class UpdateAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .addTable(TableDefinitions.USER_TABLE_CLUSTERED_BY_ONLY_DEFINITION)
            .addTable(
                TableDefinitions.TEST_PARTITIONED_TABLE_DEFINITION,
                TableDefinitions.TEST_PARTITIONED_TABLE_PARTITIONS)
            .addTable(
                "create table doc.nestedclustered (" +
                "   obj object as (" +
                "       name string" +
                "   )," +
                "   other_obj object" +
                ") clustered by (obj['name']) "
            )
            .addTable(
                "create table doc.t_nested_pk (" +
                "   o object as (" +
                "       x integer primary key," +
                "       y integer" +
                "   )" +
                ")"
            )
            .addTable("create table bag (id short primary key, ob array(object))")
            .addTable(
                "create table doc.parted_generated_column (" +
                "   ts timestamp with time zone," +
                "   day as date_trunc('day', ts)" +
                ") partitioned by (day) "
            )
            .addTable(
                "create table doc.nested_parted_generated_column (" +
                "   \"user\" object as (" +
                "       name string" +
                "   )," +
                "   name as concat(\"user\"['name'], 'bar')" +
                ") partitioned by (name) "
            );
    }

    protected AnalyzedUpdateStatement analyze(String statement) {
        return e.analyze(statement);
    }

    @Test
    public void testUpdateAnalysis() throws Exception {
        AnalyzedStatement analyzedStatement = analyze("update users set name='Ford Prefect'");
        assertThat(analyzedStatement).isExactlyInstanceOf(AnalyzedUpdateStatement.class);
    }

    @Test
    public void testUpdateUnknownTable() throws Exception {
        assertThatThrownBy(() -> analyze("update unknown set name='Prosser'"))
            .isExactlyInstanceOf(RelationUnknown.class);
    }

    @Test
    public void testUpdateSetColumnToColumnValue() throws Exception {
        AnalyzedUpdateStatement update = analyze("update users set name=name");
        assertThat(update.assignmentByTargetCol()).hasSize(1);
        Symbol value = update.assignmentByTargetCol().entrySet().iterator().next().getValue();
        assertThat(value).isReference().hasName("name");
    }

    @Test
    public void testUpdateSetExpression() throws Exception {
        AnalyzedUpdateStatement update = analyze("update users set other_id=other_id+1");
        assertThat(update.assignmentByTargetCol()).hasSize(1);
        Symbol value = update.assignmentByTargetCol().entrySet().iterator().next().getValue();
        assertThat(value).isFunction("add");
    }

    @Test
    public void testUpdateSameReferenceRepeated() throws Exception {
        assertThatThrownBy(() -> analyze("update users set name='Trillian', name='Ford'"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Target expression repeated: name");
    }

    @Test
    public void testUpdateSameNestedReferenceRepeated() throws Exception {
        assertThatThrownBy(() -> analyze("update users set details['arms']=3, details['arms']=5"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Target expression repeated: details['arms']");
    }

    @Test
    public void testUpdateSysTables() throws Exception {
        assertThatThrownBy(() -> analyze("update sys.nodes set fs={\"free\"=0}"))
            .isExactlyInstanceOf(OperationOnInaccessibleRelationException.class)
            .hasMessage("The relation \"sys.nodes\" doesn't support or allow UPDATE operations");
    }

    @Test
    public void testNumericTypeOutOfRange() {
        assertThatThrownBy(() -> analyze("update users set shorts=-100000"))
            .isExactlyInstanceOf(ColumnValidationException.class)
            .hasMessage(
                "Validation failed for shorts: Cannot cast expression `-100000` of type `integer` to `smallint`");
    }

    @Test
    public void testNumericOutOfRangeFromFunction() {
        assertThatThrownBy(() -> analyze("update users set bytes=abs(-1234)"))
            .isExactlyInstanceOf(ColumnValidationException.class)
            .hasMessage("Validation failed for bytes: Cannot cast expression `1234` of type `integer` to `byte`");
    }

    @Test
    public void testUpdateAssignments() throws Exception {
        AnalyzedUpdateStatement update = analyze("update users set name='Trillian'");
        assertThat(update.assignmentByTargetCol()).hasSize(1);
        assertThat(((DocTableRelation) update.table()).tableInfo().ident()).isEqualTo(new RelationName(Schemas.DOC_SCHEMA_NAME, "users"));

        Reference ref = update.assignmentByTargetCol().keySet().iterator().next();
        assertThat(ref.ident().tableIdent().name()).isEqualTo("users");
        assertThat(ref.column().name()).isEqualTo("name");
        assertThat(update.assignmentByTargetCol().containsKey(ref)).isTrue();

        Symbol value = update.assignmentByTargetCol().entrySet().iterator().next().getValue();
        assertThat(value).isLiteral("Trillian");
    }

    @Test
    public void testUpdateAssignmentNestedDynamicColumn() throws Exception {
        AnalyzedUpdateStatement update = analyze("update users set details['arms']=3");
        assertThat(update.assignmentByTargetCol()).hasSize(1);

        Reference ref = update.assignmentByTargetCol().keySet().iterator().next();
        assertThat(ref).isExactlyInstanceOf(DynamicReference.class);
        assertThat(ref.valueType()).isEqualTo(DataTypes.INTEGER);
        assertThat(ref.column().isRoot()).isFalse();
        assertThat(ref.column().fqn()).isEqualTo("details.arms");
    }

    @Test
    public void testUpdateAssignmentWrongType() throws Exception {
        assertThatThrownBy(() -> analyze("update users set other_id='String'"))
            .isExactlyInstanceOf(ColumnValidationException.class);
    }

    @Test
    public void testUpdateAssignmentConvertableType() throws Exception {
        AnalyzedUpdateStatement update = analyze("update users set other_id=9.9");
        Reference ref = update.assignmentByTargetCol().keySet().iterator().next();
        assertThat(ref).isNotInstanceOf(DynamicReference.class);
        assertThat(ref.valueType()).isEqualTo(DataTypes.LONG);

        Assignments assignments = Assignments.convert(update.assignmentByTargetCol(), e.nodeCtx);
        Symbol[] sources = assignments.bindSources(
            ((DocTableInfo) update.table().tableInfo()), Row.EMPTY, SubQueryResults.EMPTY);
        assertThat(sources[0]).isLiteral(9L);
    }

    @Test
    public void testUpdateMuchAssignments() throws Exception {
        AnalyzedUpdateStatement update = analyze(
            "update users set other_id=9.9, name='Trillian', details={}, stuff=true, foo='bar'");
        assertThat(update.assignmentByTargetCol()).hasSize(5);
    }

    @Test
    public void testNoWhereClause() throws Exception {
        AnalyzedUpdateStatement update = analyze("update users set other_id=9");
        assertThat(update.query()).isLiteral(true);
    }

    @Test
    public void testNoMatchWhereClause() throws Exception {
        AnalyzedUpdateStatement update = analyze("update users set other_id=9 where true=false");
        assertThat(update.query()).isLiteral(false);
    }

    @Test
    public void testUpdateWhereClause() throws Exception {
        AnalyzedUpdateStatement update = analyze("update users set other_id=9 where name='Trillian'");
        assertThat(update.query()).isFunction(EqOperator.NAME, isReference("name"), isLiteral("Trillian"));
    }

    @Test
    public void testQualifiedNameReference() throws Exception {
        assertThatThrownBy(() -> analyze("update users set users.name='Trillian'"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage(
                "Column reference \"users.name\" has too many parts. A column must not have a schema or a table here.");
    }

    @Test
    public void testUpdateWithParameter() throws Exception {
        AnalyzedUpdateStatement update = analyze("update users set name=?, other_id=?, friends=? where id=?");

        RelationName usersRelation = new RelationName("doc", "users");
        assertThat(update.assignmentByTargetCol()).hasSize(3);
        DocTableInfo tableInfo = e.schemas().getTableInfo(usersRelation);
        Reference name = tableInfo.getReference(ColumnIdent.of("name"));
        Reference friendsRef = tableInfo.getReference(ColumnIdent.of("friends"));
        Reference otherId = tableInfo.getReference(ColumnIdent.of("other_id"));
        assertThat(update.assignmentByTargetCol().get(name)).isExactlyInstanceOf(ParameterSymbol.class);
        assertThat(update.assignmentByTargetCol().get(friendsRef)).isExactlyInstanceOf(ParameterSymbol.class);
        assertThat(update.assignmentByTargetCol().get(otherId)).isExactlyInstanceOf(ParameterSymbol.class);

        assertThat(update.query())
            .isFunction(EqOperator.NAME, isReference("id"), exactlyInstanceOf(ParameterSymbol.class));
    }

    @Test
    public void testUpdateWithWrongParameters() throws Exception {
        Object[] params = {
            List.of(new HashMap<String, Object>()),
            new Map[0],
            new Long[] { 1L, 2L, 3L }
        };
        AnalyzedUpdateStatement update = analyze("update users set name=?, friends=? where other_id=?");

        Assignments assignments = Assignments.convert(update.assignmentByTargetCol(), e.nodeCtx);
        assertThatThrownBy(() -> assignments.bindSources(
                ((DocTableInfo) update.table().tableInfo()),
                new RowN(params),
                SubQueryResults.EMPTY))
            .isExactlyInstanceOf(ConversionException.class)
            .hasMessage("Cannot cast value `[{}]` to type `text`");
    }

    @Test
    public void testUpdateWithEmptyObjectArray() throws Exception {
        Object[] params = {new Map[0], 0};
        AnalyzedUpdateStatement update = analyze("update users set friends=? where other_id=0");

        Assignments assignments = Assignments.convert(update.assignmentByTargetCol(), e.nodeCtx);
        Symbol[] sources = assignments.bindSources(((DocTableInfo) update.table().tableInfo()), new RowN(params), SubQueryResults.EMPTY);

        assertThat(sources[0].valueType().id()).isEqualTo(ArrayType.ID);
        assertThat(((ArrayType<?>) sources[0].valueType()).innerType().id()).isEqualTo(ObjectType.ID);
        assertThat(((List<?>) ((Literal<?>) sources[0]).value())).hasSize(0);
    }

    @Test
    public void testUpdateSystemColumn() throws Exception {
        assertThatThrownBy(() -> analyze("update users set _id=1"))
            .isExactlyInstanceOf(ColumnValidationException.class)
            .hasMessage("Validation failed for _id: Updating a system column is not supported");
    }

    @Test
    public void testUpdatePrimaryKey() throws Exception {
        assertThatThrownBy(() -> analyze("update users set id=1"))
            .isExactlyInstanceOf(ColumnValidationException.class);
    }

    @Test
    public void testUpdateClusteredBy() throws Exception {
        assertThatThrownBy(() -> analyze("update users_clustered_by_only set id=1"))
            .isExactlyInstanceOf(ColumnValidationException.class)
            .hasMessage("Validation failed for id: Updating a clustered-by column is not supported");
    }

    @Test
    public void testUpdatePartitionedByColumn() throws Exception {
        assertThatThrownBy(() -> analyze("update parted set date = 1395874800000"))
            .isExactlyInstanceOf(ColumnValidationException.class)
            .hasMessage("Validation failed for date: Updating a partitioned-by column is not supported");
    }

    @Test
    public void testUpdatePrimaryKeyIfNestedDoesNotWork() throws Exception {
        assertThatThrownBy(() -> analyze("update t_nested_pk set o = {y=10}"))
            .isExactlyInstanceOf(ColumnValidationException.class);
    }

    @Test
    public void testUpdateColumnReferencedInGeneratedPartitionByColumn() throws Exception {
        assertThatThrownBy(() -> analyze("update parted_generated_column set ts = 1449999900000"))
            .isExactlyInstanceOf(ColumnValidationException.class)
            .hasMessageContaining(
                "Updating a column which is referenced in a partitioned by generated column expression is not supported");
    }

    @Test
    public void testUpdateColumnReferencedInGeneratedPartitionByColumnNestedParent() throws Exception {
        assertThatThrownBy(() -> analyze("update nested_parted_generated_column set \"user\" = {name = 'Ford'}"))
            .isExactlyInstanceOf(ColumnValidationException.class)
            .hasMessageContaining(
                "Updating a column which is referenced in a partitioned by generated column expression is not supported");
    }

    @Test
    public void testUpdateTableAlias() throws Exception {
        AnalyzedUpdateStatement expected = analyze("update users set awesome=true where awesome=false");
        AnalyzedUpdateStatement actual = analyze("update users as u set awesome=true where awesome=false");

        assertThat(expected.assignmentByTargetCol()).isEqualTo(actual.assignmentByTargetCol());
        assertThat(expected.query()).isEqualTo(actual.query());
    }

    @Test
    public void testUpdateObjectArrayField() throws Exception {
        assertThatThrownBy(() -> analyze("update users set friends['id'] = ?"))
            .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testWhereClauseObjectArrayField() throws Exception {
        assertThatThrownBy(() -> analyze("update users set awesome=true where friends['id'] = 5"))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessage("Invalid arguments in: (doc.users.friends['id'] = 5) with (bigint_array, integer). Valid types: (E, E)");
    }

    @Test
    public void testUpdateWithFQName() throws Exception {
        assertThatThrownBy(() -> analyze("update users set users.name = 'Ford Mustang'"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage(
                "Column reference \"users.name\" has too many parts. A column must not have a schema or a table here.");
    }

    @Test
    public void testUpdateDynamicNestedArrayParamLiteral() throws Exception {
        AnalyzedUpdateStatement update = analyze("update users set new=[[1.9, 4.8], [9.7, 12.7]]");
        DataType<?> dataType = update.assignmentByTargetCol().values().iterator().next().valueType();
        assertThat(dataType).isEqualTo(new ArrayType<>(new ArrayType<>(DoubleType.INSTANCE)));
    }

    @Test
    public void testUpdateDynamicNestedArrayParam() throws Exception {
        Object[] params = {
            new Object[] {
                new Object[] { 1.9, 4.8 },
                new Object[] { 9.7, 12.7 }
            }
        };
        AnalyzedUpdateStatement update = analyze("update users set new=? where id=1");
        Assignments assignments = Assignments.convert(update.assignmentByTargetCol(), e.nodeCtx);
        Symbol[] sources = assignments.bindSources(
                ((DocTableInfo) update.table().tableInfo()), new RowN(params), SubQueryResults.EMPTY);

        DataType<?> dataType = sources[0].valueType();
        assertThat(dataType).isEqualTo(new ArrayType<>(new ArrayType<>(DoubleType.INSTANCE)));
    }

    @Test
    public void testUpdateInvalidType() throws Exception {
        Object[] params = {
            new Object[] {
                new Object[] { "a", "b" }
            }
        };
        AnalyzedUpdateStatement update = analyze("update users set tags=? where id=1");

        Assignments assignments = Assignments.convert(update.assignmentByTargetCol(), e.nodeCtx);
        assertThatThrownBy(() -> assignments.bindSources(
                ((DocTableInfo) update.table().tableInfo()),
                new RowN(params),
                SubQueryResults.EMPTY))
            .isExactlyInstanceOf(ConversionException.class)
            .hasMessage("Cannot cast value `[[a, b]]` to type `text_array`");
    }

    @Test
    public void testUsingFQColumnNameShouldBePossibleInWhereClause() throws Exception {
        AnalyzedUpdateStatement update = analyze("update users set name = 'foo' where users.name != 'foo'");
        assertThat(update.query())
            .isFunction(NotPredicate.NAME, isFunction(EqOperator.NAME, isReference("name"), isLiteral("foo")));
    }

    @Test
    public void testTestUpdateOnTableWithAliasAndFQColumnNameInWhereClause() throws Exception {
        AnalyzedUpdateStatement update = analyze("update users  t set name = 'foo' where t.name != 'foo'");
        assertThat(update.query())
            .isFunction(NotPredicate.NAME, isFunction(EqOperator.NAME, isReference("name"), isLiteral("foo")));
    }

    @Test
    public void testUpdateNestedClusteredByColumn() throws Exception {
        assertThatThrownBy(() -> analyze("update nestedclustered set obj = {name='foobar'}"))
            .isExactlyInstanceOf(ColumnValidationException.class)
            .hasMessage("Validation failed for obj: Updating a clustered-by column is not supported");
    }

    @Test
    public void testUpdateNestedClusteredByColumnWithOtherObject() throws Exception {
        assertThatThrownBy(() -> analyze("update nestedclustered set obj = other_obj"))
            .isExactlyInstanceOf(ColumnValidationException.class)
            .hasMessage("Validation failed for obj: Updating a clustered-by column is not supported");
    }

    @Test
    public void testUpdateWhereVersionUsingWrongOperator() throws Exception {
        String stmt = "update users set text = ? where text = ? and \"_version\" >= ?";
        assertThatThrownBy(() -> e.execute(stmt, "already in panic", "don't panic", 3).getResult())
            .isExactlyInstanceOf(VersioningValidationException.class)
            .hasMessage(VersioningValidationException.VERSION_COLUMN_USAGE_MSG);
    }

    @Test
    public void testUpdateWhereVersionIsColumn() throws Exception {
        assertThatThrownBy(() -> e.execute("update users set col2 = ? where _version = id", 1).getResult())
            .isExactlyInstanceOf(VersioningValidationException.class)
            .hasMessage(VersioningValidationException.VERSION_COLUMN_USAGE_MSG);
    }

    @Test
    public void testUpdateWhereVersionInOperatorColumn() throws Exception {
        assertThatThrownBy(() -> e.execute("update users set col2 = 'x' where _version in (1,2,3)").getResult())
            .isExactlyInstanceOf(VersioningValidationException.class)
            .hasMessage(VersioningValidationException.VERSION_COLUMN_USAGE_MSG);
    }

    @Test
    public void testUpdateWhereVersionOrOperatorColumn() throws Exception {
        assertThatThrownBy(() -> e.execute("update users set col2 = ? where _version = 1 or _version = 2", 1).getResult())
            .isExactlyInstanceOf(VersioningValidationException.class)
            .hasMessage(VersioningValidationException.VERSION_COLUMN_USAGE_MSG);
    }


    @Test
    public void testUpdateWhereVersionAddition() throws Exception {
        assertThatThrownBy(() -> e.execute("update users set col2 = ? where _version + 1 = 2", 1).getResult())
            .isExactlyInstanceOf(VersioningValidationException.class)
            .hasMessage(VersioningValidationException.VERSION_COLUMN_USAGE_MSG);
    }

    @Test
    public void testUpdateWhereVersionNotPredicate() throws Exception {
        assertThatThrownBy(() -> e.execute("update users set text = ? where not (_version = 1 and id = 1)", 1).getResult())
            .isExactlyInstanceOf(VersioningValidationException.class)
            .hasMessage(VersioningValidationException.VERSION_COLUMN_USAGE_MSG);
    }

    @Test
    public void testUpdateWhereVersionOrOperator() throws Exception {
        assertThatThrownBy(() -> e.execute("update users set awesome = true where _version = 1 or _version = 2").getResult())
            .isExactlyInstanceOf(VersioningValidationException.class)
            .hasMessage(VersioningValidationException.VERSION_COLUMN_USAGE_MSG);
    }

    @Test
    public void testUpdateWithVersionZero() throws Exception {
        assertThatThrownBy(() -> e.execute("update users set awesome=true where name='Ford' and _version=0").getResult())
            .isExactlyInstanceOf(VersioningValidationException.class)
            .hasMessage(VersioningValidationException.VERSION_COLUMN_USAGE_MSG);
    }

    @Test
    public void testSelectWhereVersionIsNullPredicate() throws Exception {
        assertThatThrownBy(() -> e.execute("update users set col2 = 'x' where _version is null").getResult())
            .isExactlyInstanceOf(VersioningValidationException.class)
            .hasMessage(VersioningValidationException.VERSION_COLUMN_USAGE_MSG);
    }

    @Test
    public void testUpdateElementOfObjectArrayUsingParameterExpressionResultsInCorrectlyTypedParameterSymbol() {
        AnalyzedUpdateStatement stmt = e.analyze("UPDATE bag SET ob = [?] WHERE id = ?");
        assertThat(stmt.assignmentByTargetCol()).hasEntrySatisfying(
            toCondition(isReference("ob", new ArrayType<>(DataTypes.UNTYPED_OBJECT))),
            toCondition(isFunction("_array")));
        assertThat(stmt.assignmentByTargetCol().values())
                .satisfiesExactly(l -> assertThat(l)
                    .isFunction("_array", singletonList(DataTypes.UNTYPED_OBJECT)));
    }

    @Test
    public void testUpdateElementOfObjectArrayUsingParameterExpressionInsideFunctionResultsInCorrectlyTypedParameterSymbol() {
        AnalyzedUpdateStatement stmt = e.analyze("UPDATE bag SET ob = array_cat([?], [{obb=1}]) WHERE id = ?");
        assertThat(stmt.assignmentByTargetCol()).hasEntrySatisfying(
            toCondition(isReference("ob", new ArrayType<>(DataTypes.UNTYPED_OBJECT))),
            toCondition(isFunction("array_cat")));
        assertThat(stmt.assignmentByTargetCol().values()).satisfiesExactly(
            isFunction("array_cat",
                isFunction("_array", List.of(ObjectType.of(ColumnPolicy.DYNAMIC).setInnerType("obb", DataTypes.INTEGER).build())),
                exactlyInstanceOf(Literal.class)));
    }

    @Test
    public void test_update_returning_with_asterisk_contains_all_columns_in_returning_clause() {
        AnalyzedUpdateStatement stmt = e.analyze(
            "UPDATE users SET name='noam' RETURNING *");
        assertThat(stmt.assignmentByTargetCol()).hasEntrySatisfying(
            toCondition(isReference("name", DataTypes.STRING)),
            toCondition(isLiteral("noam")));
        assertThat(stmt.outputs()).hasSize(17);
    }

    @Test
    public void test_update_returning_with_single_value_in_returning_clause() {
        AnalyzedUpdateStatement stmt = e.analyze(
            "UPDATE users SET name='noam' RETURNING id AS foo");
        assertThat(stmt.assignmentByTargetCol()).hasEntrySatisfying(
            toCondition(isReference("name", DataTypes.STRING)),
            toCondition(isLiteral("noam")));
        assertThat(stmt.outputs()).satisfiesExactly(isAlias("foo", isReference("id")));
    }

    @Test
    public void test_update_returning_with_multiple_values_in_returning_clause() {
        AnalyzedUpdateStatement stmt = e.analyze(
            "UPDATE users SET name='noam' RETURNING id AS foo, name AS bar");
        assertThat(stmt.assignmentByTargetCol()).hasEntrySatisfying(
            toCondition(isReference("name", DataTypes.STRING)),
            toCondition(isLiteral("noam")));
        assertThat(stmt.outputs()).satisfiesExactly(
            isAlias("foo", isReference("id")), isAlias("bar", isReference("name")));
    }

    @Test
    public void test_updat_returning_with_invalid_column_returning_error() {
        assertThatThrownBy(() -> e.analyze("UPDATE users SET name='noam' RETURNING invalid"))
            .isExactlyInstanceOf(ColumnUnknownException.class)
            .hasMessage("Column invalid unknown");
    }

    @Test
    public void test_update_returning_with_single_value_altered_in_returning_clause() {
        AnalyzedUpdateStatement stmt = e.analyze(
            "UPDATE users SET name='noam' RETURNING id + 1 AS foo");
        assertThat(stmt.assignmentByTargetCol()).hasEntrySatisfying(
            toCondition(isReference("name", DataTypes.STRING)),
            toCondition(isLiteral("noam")));
        assertThat(stmt.outputs()).satisfiesExactly(
            isAlias("foo", isFunction("add", isReference("id"), isLiteral(1L))));
    }

    @Test
    public void test_update_returning_with_multiple_values_altered_in_returning_clause() {
        AnalyzedUpdateStatement stmt = e.analyze(
            "UPDATE users SET name='noam' RETURNING id + 1 AS foo, id -1 as bar");
        assertThat(stmt.assignmentByTargetCol()).hasEntrySatisfying(
                toCondition(isReference("name", DataTypes.STRING)),
                toCondition(isLiteral("noam")));
        assertThat(stmt.outputs()).satisfiesExactly(
            isAlias("foo", isFunction("add", isReference("id"), isLiteral(1L))),
            isAlias("bar", isFunction("subtract")));
    }

    @Test
    public void test_using_array_literal_as_a_left_side_of_an_assignment() {
        assertThatThrownBy(() -> e.analyze("UPDATE users SET [1][1] = 1"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("cannot use expression [1][1] as a left side of an assignment");
    }

    @Test
    public void test_update_object_columns_array_fields_by_element() throws IOException {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table t (o object as (a int[]))");
        AnalyzedUpdateStatement stmt = e.analyze("update t set o['a'][1] = 10;");
        assertThat(stmt.assignmentByTargetCol()).hasEntrySatisfying(
            toCondition(isReference("o['a']", DataTypes.INTEGER_ARRAY)),
            toCondition(isFunction("array_set",
                                   isReference("o['a']"),
                                   isFunction("_array", isLiteral(1)),
                                   isFunction("_array", isLiteral(10)))));
    }

    @Test
    public void test_update_array_of_objects_subarray_by_elements() throws IOException {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table t (a array(object as (b int[])))");

        assertThatThrownBy(() -> e.analyze("update t set a['b'][1][1] = 10;"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("cannot use expression \"a\"['b'][1][1] as a left side of an assignment");
        assertThatThrownBy(() -> e.analyze("update t set a[1][1]['b'] = 10;"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("cannot use expression \"a\"[1][1]['b'] as a left side of an assignment");
        assertThatThrownBy(() -> e.analyze("update t set a[1]['b'][1] = 10;"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("cannot use expression \"a\"[1]['b'][1] as a left side of an assignment");

        assertThatThrownBy(() -> e.analyze("update t set a['b'][1]::array(integer)[1] = 10;"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("cannot use expression CAST(\"a\"['b'][1] AS ARRAY(integer))[1] as a left side of an assignment");
        assertThatThrownBy(() -> e.analyze("update t set a[1]['b']::array(integer)[1] = 10;"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("cannot use expression CAST(\"a\"[1]['b'] AS ARRAY(integer))[1] as a left side of an assignment");
        assertThatThrownBy(() -> e.analyze("update t set a[1]::object['b'][1] = 10;"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("cannot use expression CAST(\"a\"[1] AS object)['b'][1] as a left side of an assignment");

        assertThatThrownBy(() -> e.analyze("update t set a[1]['b'] = [10];"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Updating fields of object arrays is not supported");
        assertThatThrownBy(() -> e.analyze("update t set a['b'][1] = [10];"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Updating fields of object arrays is not supported");

        assertThatThrownBy(() -> e.analyze("update t set a['b'] = [[1]];"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Updating fields of object arrays is not supported");
    }

    @Test
    public void test_update_array_of_strict_objects_by_elements_dynamically() throws IOException {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table t (a array(object(strict)))");

        assertThatThrownBy(() -> e.analyze("update t set a[1] = {c=1}"))
            .isExactlyInstanceOf(ColumnUnknownException.class)
            .hasMessage("Column a['c'] unknown");

        assertThatThrownBy(() -> e.analyze("update t set a[1]['val1']['val2'] = true"))
            .isExactlyInstanceOf(ColumnUnknownException.class)
            .hasMessage("Column a['val1']['val2'] unknown");
    }

    @Test
    public void test_update_array_of_dynamic_objects_by_elements_dynamically() throws IOException {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table t (a array(object(dynamic)))");

        AnalyzedUpdateStatement stmt = e.analyze("update t set a[1] = {c=1}");
        assertThat(stmt.assignmentByTargetCol()).hasEntrySatisfying(
            toCondition(isReference("a", new ArrayType<>(DataTypes.UNTYPED_OBJECT))),
            toCondition(isFunction("array_set",
                isFunction("_cast", isReference("a"), isLiteral("array(object(text,\"c\" integer))")),
                isFunction("_array", isLiteral(1)),
                isFunction("_array", isLiteral(Map.of("c", 1))))));

        assertThatThrownBy(() -> e.analyze("update t set a[1]['val1']['val2'] = true"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Updating fields of object arrays is not supported");
    }

    @Test
    public void test_repeated_updates_to_the_same_array() throws IOException {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table t (a int[])");
        assertThatThrownBy(() -> e.analyze("update t set a = [0,0,0], a[1] = 1"))
            .hasMessage("Target expression repeated: a")
            .isExactlyInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> e.analyze("update t set a[1] = 1, a = [0,0,0]"))
            .hasMessage("Target expression repeated: a")
            .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void test_non_deterministic_function_is_not_normalized() {
        AnalyzedUpdateStatement analyzedUpdateStatement =
            e.analyze("update users set date = curdate() where id < curdate()");
        assertThat(analyzedUpdateStatement.query())
            .isFunction(
                LtOperator.NAME,
                isReference("id"),
                isFunction(
                    ImplicitCastFunction.NAME,
                    isFunction(CurrentDateFunction.NAME),
                    isLiteral("bigint")
                )
            );
        assertThat(analyzedUpdateStatement.assignmentByTargetCol().values())
            .satisfiesExactly(
                isFunction(
                    ImplicitCastFunction.NAME,
                    isFunction(CurrentDateFunction.NAME),
                    isLiteral("timestamp with time zone")
                )
            );
    }
}
