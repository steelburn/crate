/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.NullInfoStream;
import org.apache.lucene.util.InfoStream;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class RecoverySourcePruneMergePolicyTests extends ESTestCase {

    @Test
    public void testPruneAll() throws IOException {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = newIndexWriterConfig();
            RecoverySourcePruneMergePolicy mp = new RecoverySourcePruneMergePolicy(
                "extra_source",
                MatchNoDocsQuery::new,
                newLogMergePolicy()
            );
            iwc.setMergePolicy(mp);
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < 20; i++) {
                    if (i > 0 && randomBoolean()) {
                        writer.flush();
                    }
                    Document doc = new Document();
                    doc.add(new StoredField("source", "hello world"));
                    doc.add(new StoredField("extra_source", "hello world"));
                    doc.add(new NumericDocValuesField("extra_source", 1));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
                writer.commit();
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    for (int i = 0; i < reader.maxDoc(); i++) {
                        Document document = reader.storedFields().document(i);
                        assertThat(document.getFields().size()).isEqualTo(1);
                        assertThat(document.getFields().get(0).name()).isEqualTo("source");
                    }
                    assertThat(reader.leaves().size()).isEqualTo(1);
                    LeafReader leafReader = reader.leaves().get(0).reader();
                    NumericDocValues extra_source = leafReader.getNumericDocValues("extra_source");
                    if (extra_source != null) {
                        assertThat(extra_source.nextDoc()).isEqualTo(DocIdSetIterator.NO_MORE_DOCS);
                    }
                    if (leafReader instanceof CodecReader codecReader && reader instanceof StandardDirectoryReader sdr) {
                        SegmentInfos segmentInfos = sdr.getSegmentInfos();
                        MergePolicy.MergeSpecification forcedMerges = mp.findForcedDeletesMerges(
                            segmentInfos,
                            new MergePolicy.MergeContext() {
                                @Override
                                public int numDeletesToMerge(SegmentCommitInfo info) {
                                    return info.info.maxDoc() - 1;
                                }

                                @Override
                                public int numDeletedDocs(SegmentCommitInfo info) {
                                    return info.info.maxDoc() - 1;
                                }

                                @Override
                                public InfoStream getInfoStream() {
                                    return new NullInfoStream();
                                }

                                @Override
                                public Set<SegmentCommitInfo> getMergingSegments() {
                                    return Collections.emptySet();
                                }
                            });
                        // don't wrap if there is nothing to do
                        assertThat(forcedMerges.merges.get(0).wrapForMerge(codecReader)).isSameAs(codecReader);
                    }
                }
            }
        }
    }

    @Test
    public void testPruneSome() throws IOException {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = newIndexWriterConfig();
            iwc.setMergePolicy(new RecoverySourcePruneMergePolicy("extra_source",
                                                                  () -> new TermQuery(new Term("even", "true")),
                                                                  iwc.getMergePolicy()));
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < 20; i++) {
                    if (i > 0 && randomBoolean()) {
                        writer.flush();
                    }
                    Document doc = new Document();
                    doc.add(new StringField("even", Boolean.toString(i % 2 == 0), Field.Store.YES));
                    doc.add(new StoredField("source", "hello world"));
                    doc.add(new StoredField("extra_source", "hello world"));
                    doc.add(new NumericDocValuesField("extra_source", 1));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
                writer.commit();
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    assertThat(reader.leaves().size()).isEqualTo(1);
                    NumericDocValues extra_source = reader.leaves().get(0).reader().getNumericDocValues("extra_source");
                    assertThat(extra_source).isNotNull();
                    for (int i = 0; i < reader.maxDoc(); i++) {
                        Document document = reader.storedFields().document(i);
                        Set<String> collect = document.getFields().stream().map(IndexableField::name).collect(Collectors.toSet());
                        assertThat(collect.contains("source")).isTrue();
                        assertThat(collect.contains("even")).isTrue();
                        if (collect.size() == 3) {
                            assertThat(collect.contains("extra_source")).isTrue();
                            assertThat(document.getField("even").stringValue()).isEqualTo("true");
                            assertThat(extra_source.nextDoc()).isEqualTo(i);
                        } else {
                            assertThat(document.getFields().size()).isEqualTo(2);
                        }
                    }
                    assertThat(extra_source.nextDoc()).isEqualTo(DocIdSetIterator.NO_MORE_DOCS);
                }
            }
        }
    }

    @Test
    public void testPruneNone() throws IOException {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = newIndexWriterConfig();
            iwc.setMergePolicy(new RecoverySourcePruneMergePolicy("extra_source",
                MatchAllDocsQuery::new, iwc.getMergePolicy()));
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < 20; i++) {
                    if (i > 0 && randomBoolean()) {
                        writer.flush();
                    }
                    Document doc = new Document();
                    doc.add(new StoredField("source", "hello world"));
                    doc.add(new StoredField("extra_source", "hello world"));
                    doc.add(new NumericDocValuesField("extra_source", 1));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
                writer.commit();
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    assertThat(reader.leaves().size()).isEqualTo(1);
                    NumericDocValues extra_source = reader.leaves().get(0).reader().getNumericDocValues("extra_source");
                    assertThat(extra_source).isNotNull();
                    for (int i = 0; i < reader.maxDoc(); i++) {
                        Document document = reader.storedFields().document(i);
                        Set<String> collect = document.getFields().stream().map(IndexableField::name).collect(Collectors.toSet());
                        assertThat(collect.contains("source")).isTrue();
                        assertThat(collect.contains("extra_source")).isTrue();
                        assertThat(extra_source.nextDoc()).isEqualTo(i);
                    }
                    assertThat(extra_source.nextDoc()).isEqualTo(DocIdSetIterator.NO_MORE_DOCS);
                }
            }
        }
    }
}
