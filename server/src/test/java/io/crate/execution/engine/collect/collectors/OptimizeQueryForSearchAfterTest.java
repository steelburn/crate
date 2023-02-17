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


package io.crate.execution.engine.collect.collectors;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import io.crate.analyze.OrderBy;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.types.DataTypes;

public class OptimizeQueryForSearchAfterTest {

    @Test
    public void test_string_range_query_can_handle_byte_ref_values() throws Exception {
        ReferenceIdent referenceIdent = new ReferenceIdent(new RelationName("doc", "dummy"), "x");
        OrderBy orderBy = new OrderBy(List.of(
            new SimpleReference(referenceIdent, RowGranularity.DOC, DataTypes.STRING, 1, null)
        ));
        var optimize = new OptimizeQueryForSearchAfter(orderBy);
        FieldDoc lastCollected = new FieldDoc(1, 1.0f, new Object[] { new BytesRef("foobar") });
        Query query = optimize.apply(lastCollected);
        assertThat(query).hasToString("+x:{* TO foobar}");
    }
}
