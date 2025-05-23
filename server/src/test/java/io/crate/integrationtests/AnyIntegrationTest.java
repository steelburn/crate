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

import static io.crate.testing.Asserts.assertThat;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

public class AnyIntegrationTest extends IntegTestCase {

    @Test
    public void testAnyOnArrayLiteralDeleteUpdateSelect() throws Exception {
        execute("create table t (" +
                "  i int," +
                "  ia array(int)," +
                "  s string," +
                "  sa array(string)" +
                ") clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (i, ia, s, sa) values (?, ?, ?, ?)", new Object[][]{
            {1, new Object[]{1, 2, 3}, "foo", new Object[]{"abc", "def", "ghi"}},
            {2, new Object[]{3, 4, 5}, "bar", new Object[]{"rst", "uvw", "aaa"}},
            {3, new Object[]{7, 8, 9}, "baz", new Object[]{"baz"}}
        });
        execute("refresh table t");

        execute("select i, s from t where i = ANY([1,2,4]) order by i");
        assertThat(response).hasRows("1| foo", "2| bar");

        execute("select i, sa from t where s not like ANY(sa) order by i");
        assertThat(response).hasRows("1| [abc, def, ghi]", "2| [rst, uvw, aaa]");

        execute("update t set s='updated' where i > ANY([?, ?, ?])", new Object[]{2, 4, 5});
        assertThat(response).hasRowCount(1);
        execute("refresh table t");

        execute("select s from t order by i");
        assertThat(response).hasRows("foo", "bar", "updated");

        execute("delete from t where s like ANY (['f%', 'ba_'])");
        assertThat(response).hasRowCount(2);
        execute("refresh table t");

        execute("select * from t");
        assertThat(response).hasRows("3| [7, 8, 9]| updated| [baz]");
    }

    @Test
    public void testAnyOnArrayLiteral() throws Exception {
        execute("create table t (b byte, sa array(string), s string) clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (b, sa, s) values (1, ['foo', 'bar'], 'goo')," +
                "(2, ['bar', 'baz'], 'zar')," +
                "(3, ['funky', 'shizzle'], 'ziffle')");
        execute("refresh table t");

        execute("select * from t where 'bar' = ANY(sa)");
        assertThat(response).hasRowCount(2);

        execute("select b from t where b = ANY([1, 2, 4]) order by b");
        assertThat(response).hasRowCount(2);
        assertThat(response.rows()[0][0]).isEqualTo((byte) 1);
        assertThat(response.rows()[1][0]).isEqualTo((byte) 2);

        execute("select * from t where b != ANY([1, 2, 4]) order by b");
        assertThat(response).hasRowCount(3); // all rows does not contain at least one of the array elements

        execute("select b from t where b <= ANY([-1, 0, 1])");
        assertThat(response).hasRowCount(1);
        assertThat(response.rows()[0][0]).isEqualTo((byte) 1);

        execute("select b from t where s like ANY(['%ar', 'go%']) order by b DESC");
        assertThat(response).hasRowCount(2);
        assertThat(response.rows()[0][0]).isEqualTo((byte) 2);
        assertThat(response.rows()[1][0]).isEqualTo((byte) 1);
    }

    @Test
    public void testAnyOnArrayLiteralWithNullElements() throws Exception {
        execute("create table t (s string)");
        ensureYellow();
        execute("insert into t (s) values ('foo'), (null)");
        execute("refresh table t");
        execute("select * from t where s = ANY (['foo', 'bar', null])");
        assertThat(response).hasRowCount(1);

        execute("select * from t where s = ANY ([null])");
        assertThat(response).hasRowCount(0);
    }

    @Test
    public void testArrayReferenceOfDifferentTypeSoThatCastIsRequired() throws Exception {
        execute("create table t (x array(short))");
        ensureYellow();

        execute("insert into t (x) values ([1, 2, 3, 4])");
        execute("refresh table t");
        execute("select * from t where 4 < ANY (x) ");
        assertThat(response).hasRowCount(0);
    }

    @Test
    public void testNotAnyInWhereClauseDoesNotFilterOutEmptyArrays() {
        execute("create table t (b integer, labels array(string))");
        execute("insert into t (b, labels) values (1, ['one', 'two'])," +
                "(2, ['two', 'three'])," +
                "(3, ['three', 'four'])," +
                "(4, [])");
        execute("refresh table t");

        execute("select b from t where not 'two' = ANY(labels) order by b");
        assertThat(response).hasRows("3", "4");
    }

    @Test
    public void testAnyOperatorWithFieldThatRequiresConversion() {
        execute(
            "create table t (" +
            "   ts timestamp with time zone" +
            ") clustered into 1 shards with (number_of_replicas = 0)");
        execute("insert into t values ('2017-12-31'), ('2016-12-31'), ('2015-12-31')");
        execute("refresh table t");

        execute("select ts from t where ts = ANY (['2017-12-31', '2016-12-31']) order by ts");
        assertThat(response).hasRowCount(2);
        assertThat(response.rows()[0][0]).isEqualTo(1483142400000L);
        assertThat(response.rows()[1][0]).isEqualTo(1514678400000L);
    }
}
