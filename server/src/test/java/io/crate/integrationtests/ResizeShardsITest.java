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
import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertSQLError;
import static io.crate.testing.Asserts.assertThat;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

public class ResizeShardsITest extends IntegTestCase {

    private String getADataNodeName(ClusterState state) {
        assertThat(state.nodes().getDataNodes()).isNotEmpty();
        return state.nodes().getDataNodes().valuesIt().next().getName();
    }

    @Test
    public void testShrinkShardsOfTable() throws Exception {
        execute(
            "create table quotes (" +
            "   id integer," +
            "   quote string," +
            "   date timestamp with time zone" +
            ") clustered into 3 shards with (number_of_replicas = 0)");

        execute("insert into quotes (id, quote, date) values (?, ?, ?), (?, ?, ?)",
            new Object[]{
                1, "Don't panic", 1395874800000L,
                2, "Now panic", 1395961200000L}
        );

        execute("refresh table quotes");

        ClusterService clusterService = cluster().getInstance(ClusterService.class);
        final String resizeNodeName = getADataNodeName(clusterService.state());

        execute("alter table quotes set (\"routing.allocation.require._name\"=?, \"blocks.write\"=?)",
            $(resizeNodeName, true));
        assertBusy(() -> {
            execute(
                "select count(*) from sys.shards where " +
                "table_name = 'quotes' " +
                "and state = 'STARTED' " +
                "and node['name'] = ?",
                new Object[] { resizeNodeName }
            );
            assertThat(response).hasRows(new Object[] { 3L });
        });

        execute("alter table quotes set (number_of_shards=?)", $(1));
        ensureYellow();

        execute("select count(*), primary from sys.shards where table_name = 'quotes' group by primary order by 2");
        assertThat(response).hasRows("1| true");
        execute("select id from quotes");
        assertThat(response).hasRowCount(2L);
    }

    @Test
    public void testShrinkShardsOfPartition() throws Exception {
        execute("create table quotes (id integer, quote string, date timestamp with time zone) " +
                "partitioned by(date) clustered into 3 shards with (number_of_replicas = 2)");
        execute("insert into quotes (id, quote, date) values (?, ?, ?), (?, ?, ?)",
            new Object[]{
                1, "Don't panic", 1395874800000L,
                2, "Now panic", 1395961200000L}
        );
        execute("refresh table quotes");

        ClusterService clusterService = cluster().getInstance(ClusterService.class);
        final String resizeNodeName = getADataNodeName(clusterService.state());

        execute("alter table quotes partition (date=1395874800000) " +
                "set (\"routing.allocation.require._name\"=?, \"blocks.write\"=?)",
            $(resizeNodeName, true));
        assertBusy(() -> {
            execute(
                "select count(*) from sys.shards where " +
                "table_name = 'quotes' " +
                "and partition_ident = '04732cpp6ks3ed1o60o30c1g' " +
                "and state = 'STARTED' " +
                "and node['name'] = ?",
                new Object[] { resizeNodeName }
            );
            assertThat(response).hasRows(new Object[] { 3L });
        });
        execute("alter table quotes partition (date=1395874800000) set (number_of_shards=?)",
            $(1));
        ensureYellow();

        execute("select partition_ident, number_of_shards from information_schema.table_partitions " +
                "where table_name = 'quotes' " +
                "and values = '{\"date\": 1395874800000}'");
        assertThat(response.rows()[0][1]).isEqualTo(1);

        String partitionIdent = (String) response.rows()[0][0];
        execute("select count(*), primary from sys.shards where table_name = 'quotes' and " + "" +
            "partition_ident='" + partitionIdent + "' group by primary order by 2");
        assertThat(response).hasRows(
            "2| false",
            "1| true");
        execute("select id from quotes");
        assertThat(response).hasRowCount(2L);
    }

    @Test
    public void testNumberOfShardsOfATableCanBeIncreased() throws Exception {
        execute("create table t1 (x int, p int) clustered into 1 shards " +
                "with (number_of_replicas = 1, number_of_routing_shards = 10)");
        execute("insert into t1 (x, p) values (1, 1), (2, 1)");

        execute("alter table t1 set (\"blocks.write\" = true)");

        execute("alter table t1 set (number_of_shards = 2)");
        ensureYellow();

        execute("select count(*), primary from sys.shards where table_name = 't1' group by primary order by 2");
        assertThat(response).hasRows(
            "2| false",
            "2| true");
        execute("select x from t1");
        assertThat(response).hasRowCount(2L);
    }

    @Test
    public void test_number_of_shards_of_a_table_can_be_increased_without_explicitly_setting_number_of_routing_shards() throws Exception {
        execute("create table t1 (x int, p int) clustered into 1 shards " +
            "with (number_of_replicas = 2)");
        execute("insert into t1 (x, p) select g, g from generate_series(1, 12, 1) as g");
        execute("refresh table t1");

        execute("alter table t1 set (\"blocks.write\" = true)");

        execute("alter table t1 set (number_of_shards = 12)");
        ensureYellow();

        execute("select count(*), primary from sys.shards where table_name = 't1' group by primary order by 2");
        assertThat(response).hasRows(
            "24| false",
            "12| true");
        execute("select x from t1");
        assertThat(response).hasRowCount(12L);
    }

    @Test
    public void test_number_of_shards_on_a_one_sharded_table_can_be_increased_without_explicitly_setting_number_of_routing_shards() throws Exception {
        execute("create table t1 (x int, p int) clustered into 1 shards " +
            "with (number_of_replicas = 2)");
        execute("insert into t1 (x, p) select g, g from generate_series(1, 12, 1) as g");
        execute("refresh table t1");

        execute("alter table t1 set (\"blocks.write\" = true)");

        execute("alter table t1 set (number_of_shards = 3)");
        ensureYellow();

        execute("select count(*), primary from sys.shards where table_name = 't1' group by primary order by 2");
        assertThat(response).hasRows(
            "6| false",
            "3| true");
        execute("select x from t1");
        assertThat(response).hasRowCount(12L);

        // number_of_routing_shards is calculated based on 3 shards (after the first increase) and set implicitly
        assertSQLError(() -> execute("alter table t1 set (number_of_shards = 8)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("the number of source shards [3] must be a must be a factor of [8]");

        execute("alter table t1 set (number_of_shards = 12)");
        ensureYellow();

        execute("select count(*), primary from sys.shards where table_name = 't1' group by primary order by 2");
        assertThat(response).hasRows(
            "24| false",
            "12| true");
        execute("select x from t1");
        assertThat(response).hasRowCount(12L);
    }

    @Test
    public void testNumberOfShardsOfAPartitionCanBeIncreased() throws Exception {
        execute("create table t_parted (x int, p int) partitioned by (p) clustered into 1 shards " +
                "with (number_of_replicas = 0, number_of_routing_shards = 10)");
        execute("insert into t_parted (x, p) values (1, 1), (2, 1)");
        execute("refresh table t_parted");
        execute("alter table t_parted partition (p = 1) set (\"blocks.write\" = true)");

        execute("alter table t_parted partition (p = 1) set (number_of_shards = 5)");
        ensureYellow();

        execute("select count(*), primary from sys.shards where table_name = 't_parted' group by primary order by 2");
        assertThat(response).hasRows(
            "5| true");
        execute("select x from t_parted");
        assertThat(response).hasRowCount(2L);
    }
}
