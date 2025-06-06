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

package io.crate.integrationtests;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static io.crate.protocols.postgres.PGErrorStatus.DUPLICATE_TABLE;
import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThat;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Test;

import io.crate.exceptions.RelationAlreadyExists;
import io.crate.metadata.RelationName;
import io.crate.metadata.view.ViewsMetadata;
import io.crate.protocols.postgres.PGErrorStatus;
import io.crate.testing.Asserts;
import io.netty.handler.codec.http.HttpResponseStatus;

public class ViewsITest extends IntegTestCase {

    @After
    public void dropViews() {
        execute("SELECT table_schema || '.' || table_name FROM information_schema.views");
        if (response.rows().length > 0) {
            String views = Stream.of(response.rows())
                .map(row -> String.valueOf(row[0]))
                .collect(Collectors.joining(", "));
            execute(String.format("DROP VIEW %s", views));
        }
    }

    @Test
    public void testViewCanBeCreatedSelectedAndThenDropped() {
        execute("create table t1 (x int)");
        execute("insert into t1 (x) values (1)");
        execute("refresh table t1");
        execute("create view v1 as select * from t1 where x > ?", $(0));
        for (ClusterService clusterService : cluster().getInstances(ClusterService.class)) {
            ViewsMetadata views = clusterService.state().metadata().custom(ViewsMetadata.TYPE);
            assertThat(views).isNotNull();
            assertThat(views.contains(RelationName.fromIndexName(sqlExecutor.getCurrentSchema() + ".v1"))).isTrue();
        }
        assertThat(execute("select * from v1")).hasRows("1");
        assertThat(execute("select view_definition from information_schema.views")).hasRows(
            """
            SELECT *
            FROM "t1"
            WHERE "x" > 0
            """
        );
        execute("drop view v1");
        for (ClusterService clusterService : cluster().getInstances(ClusterService.class)) {
            ViewsMetadata views = clusterService.state().metadata().custom(ViewsMetadata.TYPE);
            assertThat(views.contains(RelationName.fromIndexName(sqlExecutor.getCurrentSchema() + ".v1"))).isFalse();
        }
    }

    @Test
    public void test_view_on_top_level_columns_sub_columns_are_shown_in_information_schema() throws Exception {
        execute("CREATE TABLE with_object (a OBJECT AS (b INTEGER), x int, y int)");
        execute("CREATE VIEW view_with_object AS SELECT x, a, y FROM with_object");

        execute("SELECT column_name, data_type, ordinal_position FROM information_schema.columns WHERE table_name = 'with_object'");
        assertThat(response).hasRows(
            "a| object| 1",
            "a['b']| integer| 2",
            "x| integer| 3",
            "y| integer| 4"
        );

        // Note, that ordinal of the column "a" in the view is different from ordinal in the table.
        // Ordinals in views assigned by select order and not related to original ordinal in table.
        // This is compatible with PG behavior.
        execute("SELECT column_name, data_type, ordinal_position FROM information_schema.columns WHERE table_name = 'view_with_object'");
        assertThat(response).hasRows(
            "x| integer| 1",
            "a| object| 2",
            "y| integer| 3",
            "a['b']| integer| 4"
        );

        // View dynamically binds new sub-columns (even if SELECT is static).
        execute("ALTER TABLE with_object ADD COLUMN a['c'] text");
        execute("SELECT column_name, data_type, ordinal_position FROM information_schema.columns WHERE table_name = 'view_with_object'");
        assertThat(response).hasRows(
            "x| integer| 1",
            "a| object| 2",
            "y| integer| 3",
            "a['b']| integer| 4",
            "a['c']| text| 5" // New columns have higher ordinals to keep all ordinals stable.
        );
    }

    @Test
    public void testViewCanBeUsedForJoins() {
        execute("CREATE TABLE t1 (a STRING, x INTEGER)");
        execute("INSERT INTO t1 (x, a) VALUES (1, 'foo')");
        execute("REFRESH TABLE t1");
        execute("CREATE VIEW v1 AS select * FROM t1");
        execute("CREATE VIEW v2 AS select * FROM t1");
        assertThat(execute("SELECT * FROM v1 INNER JOIN v2 ON v1.x = v2.x")).hasRows("foo| 1| foo| 1");
    }

    @Test
    public void testViewCanBeCreatedAndThenReplaced() {
        execute("create view v2 as select 1 from sys.cluster");
        assertThat(execute("select * from v2")).hasRows("1");
        execute("create or replace view v2 as select 2 from sys.cluster");
        assertThat(execute("select * from v2")).hasRows("2");
        for (ClusterService clusterService : cluster().getInstances(ClusterService.class)) {
            ViewsMetadata views = clusterService.state().metadata().custom(ViewsMetadata.TYPE);
            assertThat(views).isNotNull();
            assertThat(views.contains(RelationName.fromIndexName(sqlExecutor.getCurrentSchema() + ".v2"))).isTrue();
        }
    }

    @Test
    public void testCreateViewFailsIfViewAlreadyExists() {
        execute("create view v3 as select 1");

        Asserts.assertSQLError(() -> execute("create view v3 as select 1"))
            .hasPGError(DUPLICATE_TABLE)
            .hasHTTPError(CONFLICT, 4093)
            .hasMessageContaining("Relation '" + sqlExecutor.getCurrentSchema() + ".v3' already exists");
    }

    @Test
    public void testCreateViewFailsIfNameConflictsWithTable() {
        execute("create table t1 (x int) clustered into 1 shards with (number_of_replicas = 0)");

        Asserts.assertSQLError(() -> execute("create view t1 as select 1"))
            .hasPGError(DUPLICATE_TABLE)
            .hasHTTPError(CONFLICT, 4093)
            .hasMessageContaining("Relation '" + sqlExecutor.getCurrentSchema() + ".t1' already exists");
    }

    @Test
    public void testCreateViewFailsIfNameConflictsWithPartitionedTable() {
        execute("create table t1 (x int) partitioned by (x) clustered into 1 shards with (number_of_replicas = 0)");

        Asserts.assertSQLError(() -> execute("create view t1 as select 1"))
            .hasPGError(DUPLICATE_TABLE)
            .hasHTTPError(CONFLICT, 4093)
            .hasMessageContaining("Relation '" + sqlExecutor.getCurrentSchema() + ".t1' already exists");
    }

    @Test
    public void testCreateTableFailsIfNameConflictsWithView() {
        // First plan the create table which should conflict with the view,
        PlanForNode viewConflictingTableCreation =
            plan("create table v4 (x int) clustered into 1 shards with (number_of_replicas = 0)");
        // then create the actual view. This way we circumvent the analyzer check for existing views.
        execute("create view v4 as select 1");

        assertThatThrownBy(() -> execute(viewConflictingTableCreation).getResult())
            .isExactlyInstanceOf(RelationAlreadyExists.class)
            .hasMessage("Relation '" + sqlExecutor.getCurrentSchema() + ".v4' already exists.");
    }

    @Test
    public void testCreatePartitionedTableFailsIfNameConflictsWithView() {
        // First plan the create table which should conflict with the view,
        PlanForNode viewConflictingTableCreation =
            plan("create table v5 (x int) clustered into 1 shards with (number_of_replicas = 0)");
        // then create the actual view. This way we circumvent the analyzer check for existing views.
        execute("create view v5 as select 1");

        assertThatThrownBy(() -> execute(viewConflictingTableCreation).getResult())
            .isExactlyInstanceOf(RelationAlreadyExists.class)
            .hasMessage("Relation '" + sqlExecutor.getCurrentSchema() + ".v5' already exists.");
    }

    @Test
    public void testDropViewFailsIfViewIsMissing() {
        Asserts.assertSQLError(() -> execute("drop view v1"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(HttpResponseStatus.NOT_FOUND, 4041)
            .hasMessageContaining("Relations not found: " + sqlExecutor.getCurrentSchema() + ".v1");
    }

    @Test
    public void testDropViewDoesNotFailIfViewIsMissingAndIfExistsIsUsed() {
        execute("drop view if exists v1");
    }

    @Test
    public void testSubscriptOnViews() {
        execute("create table t1 (a object as (b integer), c object as (d object as (e integer))) ");
        execute("insert into t1 (a, c) values ({ b = 1 }, { d = { e = 2 }})");
        execute("refresh table t1");
        execute("create view v1 as select * from t1");
        // must not throw an exception, subscript must be resolved
        execute("select a['b'], c['d']['e'] from v1");
        assertThat(response).hasRows("1| 2");
    }

    @Test
    public void test_where_clause_on_view_normalized_on_coordinator_node() {
        execute("create table test (x timestamp, y int)");
        execute("create view v_test as select * from test");
        execute("select * from v_test where x > current_timestamp - INTERVAL '24' HOUR");
    }


    @Test
    public void test_creating_a_self_referencing_view_is_not_allowed() {
        execute("create view v as select * from sys.cluster");
        Asserts.assertSQLError(() -> execute("create or replace view v as select * from v"))
                .hasPGError(INTERNAL_ERROR)
                .hasHTTPError(HttpResponseStatus.BAD_REQUEST, 4000)
                .hasMessageContaining("Creating a view that references itself is not allowed");
    }

    @Test
    public void test_can_rename_existing_view() throws Exception {
        execute("create view v1 as select * from sys.cluster");
        assertThat(execute("select * from v1")).hasRowCount(1);

        execute("alter table v1 rename to v2");
        assertThat(execute("select * from v2")).hasRowCount(1);
        Asserts.assertSQLError(() -> execute("select * from v1"))
            .hasPGError(PGErrorStatus.UNDEFINED_TABLE)
            .hasHTTPError(HttpResponseStatus.NOT_FOUND, 4041)
            .hasMessageContaining("Relation 'v1' unknown");

        assertThat(execute("select table_name from information_schema.views")).hasRows(
            "v2"
        );
    }

    @Test
    public void test_cannot_rename_view_if_target_already_exists() {
        String schema = sqlExecutor.getCurrentSchema();

        execute("create view v1 as select * from sys.cluster");
        assertThat(execute("select * from v1")).hasRowCount(1);
        execute("create view v2 as select * from sys.cluster");
        assertThat(execute("select * from v2")).hasRowCount(1);
        Asserts.assertSQLError(() -> execute("alter table v1 rename to v2"))
            .hasPGError(PGErrorStatus.INTERNAL_ERROR)
            .hasHTTPError(HttpResponseStatus.BAD_REQUEST, 4000)
            .hasMessageContaining(String.format(
                Locale.ENGLISH,
                "Cannot rename view %s.v1 to %s.v2, view %s.v2 already exists",
                schema, schema, schema));

        execute("create table tbl(a int)");
        Asserts.assertSQLError(() -> execute("alter table v1 rename to tbl"))
            .hasPGError(PGErrorStatus.INTERNAL_ERROR)
            .hasHTTPError(HttpResponseStatus.BAD_REQUEST, 4000)
            .hasMessageContaining(String.format(
                Locale.ENGLISH,
                "Cannot rename view %s.v1 to %s.tbl, table %s.tbl already exists",
                schema, schema, schema));

        execute("create table tbl_parted(a int) partitioned by(a)");
        Asserts.assertSQLError(() -> execute("alter table v1 rename to tbl_parted"))
            .hasPGError(PGErrorStatus.INTERNAL_ERROR)
            .hasHTTPError(HttpResponseStatus.BAD_REQUEST, 4000)
            .hasMessageContaining(String.format(
                Locale.ENGLISH,
                "Cannot rename view %s.v1 to %s.tbl_parted, table %s.tbl_parted already exists",
                schema, schema, schema));
    }

    @Test
    public void test_object_column_of_view_contain_inner_types() throws Exception {
        execute("CREATE TABLE data (value_1 BOOLEAN, value_2 INTEGER)");
        execute("CREATE VIEW data_view AS " +
            "SELECT { value_1 = value_1, value_2 = value_2 } AS o FROM data");
        execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'data_view' ORDER BY ordinal_position");
        assertThat(response).hasRows(
            "o| object",
            "o['value_1']| boolean",
            "o['value_2']| integer"
        );
    }

    /*
     * https://github.com/crate/crate/issues/17836
     */
    @Test
    public void test_error_on_unknown_object_key_is_persisted() {
        try (var session = sqlExecutor.newSession()) {
            execute("SET error_on_unknown_object_key=false", session);
            execute("CREATE TABLE doc.tbl1 (obj OBJECT(DYNAMIC))", session);
            execute("CREATE VIEW vw1 AS SELECT obj['not_existing'] FROM doc.tbl1;", session);
            execute("SET error_on_unknown_object_key=true", session);

            execute("select view_definition from information_schema.views where table_name='vw1';", session);
            assertThat(response).hasRows(
                "SELECT \"obj\"['not_existing']\n" +
                    "FROM \"doc\".\"tbl1\"\n"
            );

            // Ensure view can be executed
            execute("SELECT * FROM vw1", session);
        }
    }
}
