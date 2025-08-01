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

package org.elasticsearch.index.translog;

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.elasticsearch.common.util.BigArrays.NON_RECYCLING_INSTANCE;
import static org.elasticsearch.index.translog.TranslogDeletionPolicies.createTranslogDeletionPolicy;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.CopyOption;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.backward_codecs.store.EndiannessReverserUtil;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.tests.mockfile.FilterFileChannel;
import org.apache.lucene.tests.mockfile.FilterFileSystemProvider;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.tests.util.LineFileDocs;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.Assertions;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.RejectableRunnable;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.Engine.Operation.Origin;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SequenceIDFields;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.seqno.LocalCheckpointTrackerTests;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog.Location;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.VersionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import io.crate.common.collections.Sets;
import io.crate.common.collections.Tuple;
import io.crate.common.io.IOUtils;
import io.crate.metadata.doc.SysColumns;

@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class TranslogTests extends ESTestCase {

    protected final ShardId shardId = new ShardId("index", "_na_", 1);

    protected Translog translog;
    private AtomicLong globalCheckpoint;
    protected Path translogDir;
    // A default primary term is used by translog instances created in this test.
    private final AtomicLong primaryTerm = new AtomicLong();
    private final AtomicReference<LongConsumer> persistedSeqNoConsumer = new AtomicReference<>();
    private boolean expectIntactTranslog;

    @Before
    public void expectIntactTranslogByDefault() {
        expectIntactTranslog = true;
    }

    @Override
    protected void afterIfSuccessful() throws Exception {
        super.afterIfSuccessful();

        if (translog.isOpen()) {
            if (translog.currentFileGeneration() > 1) {
                translog.getDeletionPolicy().setLocalCheckpointOfSafeCommit(Long.MAX_VALUE);
                translog.trimUnreferencedReaders();
                assertFileDeleted(translog, translog.currentFileGeneration() - 1);
            }
            translog.close();
        }
        if (expectIntactTranslog) {
            assertFileIsPresent(translog, translog.currentFileGeneration());
        }
        IOUtils.rm(translog.location()); // delete all the locations

    }

    private LongConsumer getPersistedSeqNoConsumer() {
        return seqNo -> {
            final LongConsumer consumer = persistedSeqNoConsumer.get();
            if (consumer != null) {
                consumer.accept(seqNo);
            }
        };
    }

    protected Translog createTranslog(TranslogConfig config) throws IOException {
        String translogUUID =
            Translog.createEmptyTranslog(config.getTranslogPath(), SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        return new Translog(config, translogUUID, createTranslogDeletionPolicy(config.getIndexSettings()),
            () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTerm::get, getPersistedSeqNoConsumer());
    }

    protected Translog openTranslog(TranslogConfig config, String translogUUID) throws IOException {
        return new Translog(config, translogUUID, createTranslogDeletionPolicy(config.getIndexSettings()),
            () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTerm::get, getPersistedSeqNoConsumer());
    }


    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        primaryTerm.set(randomLongBetween(1, Integer.MAX_VALUE));
        // if a previous test failed we clean up things here
        translogDir = createTempDir();
        translog = create(translogDir);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        try {
            translog.getDeletionPolicy().assertNoOpenTranslogRefs();
            translog.close();
        } finally {
            super.tearDown();
        }
    }

    private Translog create(Path path) throws IOException {
        globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        final TranslogConfig translogConfig = getTranslogConfig(path);
        final TranslogDeletionPolicy deletionPolicy = createTranslogDeletionPolicy(translogConfig.getIndexSettings());
        final String translogUUID = Translog.createEmptyTranslog(path, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        return new Translog(translogConfig, translogUUID, deletionPolicy, () -> globalCheckpoint.get(), primaryTerm::get,
            getPersistedSeqNoConsumer());
    }

    private TranslogConfig getTranslogConfig(final Path path) {
        final Settings settings = Settings
            .builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.elasticsearch.Version.CURRENT)
            // only randomize between nog age retention and a long one, so failures will have a chance of reproducing
            .put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), randomBoolean() ? "-1ms" : "1h")
            .put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), randomIntBetween(-1, 2048) + "b")
            .build();
        return getTranslogConfig(path, settings);
    }

    private TranslogConfig getTranslogConfig(final Path path, final Settings settings) {
        final ByteSizeValue bufferSize = randomFrom(
            TranslogConfig.DEFAULT_BUFFER_SIZE,
            new ByteSizeValue(8, ByteSizeUnit.KB),
            new ByteSizeValue(10 + randomInt(128 * 1024), ByteSizeUnit.BYTES));

        final IndexSettings indexSettings =
            IndexSettingsModule.newIndexSettings(shardId.getIndex(), settings);
        return new TranslogConfig(shardId, path, indexSettings, NON_RECYCLING_INSTANCE, bufferSize);
    }

    private Location addToTranslogAndList(Translog translog, List<Translog.Operation> list, Translog.Operation op) throws IOException {
        list.add(op);
        return translog.add(op);
    }

    @Test
    public void testIdParsingFromFile() {
        long id = randomIntBetween(0, Integer.MAX_VALUE);
        Path file = translogDir.resolve(Translog.TRANSLOG_FILE_PREFIX + id + ".tlog");
        assertThat(Translog.parseIdFromFileName(file)).isEqualTo(id);

        id = randomIntBetween(0, Integer.MAX_VALUE);
        file = translogDir.resolve(Translog.TRANSLOG_FILE_PREFIX + id);
        try {
            Translog.parseIdFromFileName(file);
            fail("invalid pattern");
        } catch (IllegalArgumentException ex) {
            // all good
        }

        file = translogDir.resolve(Translog.TRANSLOG_FILE_PREFIX + id + ".recovering");
        try {
            Translog.parseIdFromFileName(file);
            fail("invalid pattern");
        } catch (IllegalArgumentException ex) {
            // all good
        }

        file = translogDir.resolve(Translog.TRANSLOG_FILE_PREFIX + randomNonTranslogPatternString(1, 10) + id);
        try {
            Translog.parseIdFromFileName(file);
            fail("invalid pattern");
        } catch (IllegalArgumentException ex) {
            // all good
        }
        file = translogDir.resolve(randomNonTranslogPatternString(1, Translog.TRANSLOG_FILE_PREFIX.length() - 1));
        try {
            Translog.parseIdFromFileName(file);
            fail("invalid pattern");
        } catch (IllegalArgumentException ex) {
            // all good
        }
    }

    private String randomNonTranslogPatternString(int min, int max) {
        String string;
        boolean validPathString;
        do {
            validPathString = false;
            string = RandomizedTest.randomRealisticUnicodeOfCodepointLength(randomIntBetween(min, max));
            try {
                final Path resolved = translogDir.resolve(string);
                // some strings (like '/' , '..') do not refer to a file, which we this method should return
                validPathString = resolved.getFileName() != null;
            } catch (InvalidPathException ex) {
                // some FS don't like our random file names -- let's just skip these random choices
            }
        } while (Translog.PARSE_STRICT_ID_PATTERN.matcher(string).matches() || validPathString == false);
        return string;
    }


    @Test
    public void testSimpleOperations() throws IOException {
        ArrayList<Translog.Operation> ops = new ArrayList<>();
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
           assertThat(snapshot).hasSize(0);
        }

        addToTranslogAndList(translog, ops, new Translog.Index("1", 0, primaryTerm.get(), new byte[]{1}));
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertThat(snapshot).equalsTo(ops);
            assertThat(snapshot.totalOperations()).isEqualTo(ops.size());
        }

        addToTranslogAndList(translog, ops, new Translog.Delete("2", 1, primaryTerm.get(), newUid("2")));
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertThat(snapshot).equalsTo(ops);
            assertThat(snapshot.totalOperations()).isEqualTo(ops.size());
        }

        final long seqNo = randomLongBetween(0, Integer.MAX_VALUE);
        final String reason = randomAlphaOfLength(16);
        final long noopTerm = randomLongBetween(1, primaryTerm.get());
        addToTranslogAndList(translog, ops, new Translog.NoOp(seqNo, noopTerm, reason));

        try (Translog.Snapshot snapshot = translog.newSnapshot()) {

            Translog.Index index = (Translog.Index) snapshot.next();
            assertThat(index).isNotNull();
            assertThat(BytesReference.toBytes(index.getSource())).isEqualTo(new byte[]{1});

            Translog.Delete delete = (Translog.Delete) snapshot.next();
            assertThat(delete).isNotNull();
            assertThat(delete.uid()).isEqualTo(newUid("2"));

            Translog.NoOp noOp = (Translog.NoOp) snapshot.next();
            assertThat(noOp).isNotNull();
            assertThat(noOp.seqNo()).isEqualTo(seqNo);
            assertThat(noOp.primaryTerm()).isEqualTo(noopTerm);
            assertThat(noOp.reason()).isEqualTo(reason);

            assertThat(snapshot.next()).isNull();
        }

        long firstId = translog.currentFileGeneration();
        translog.rollGeneration();
        assertThat(translog.currentFileGeneration()).isNotEqualTo(firstId);

        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertThat(snapshot).equalsTo(ops);
            assertThat(snapshot.totalOperations()).isEqualTo(ops.size());
        }

        try (Translog.Snapshot snapshot = translog.newSnapshot(seqNo + 1, randomLongBetween(seqNo + 1, Long.MAX_VALUE))) {
           assertThat(snapshot).hasSize(0);
            assertThat(snapshot.totalOperations()).isEqualTo(0);
        }
    }

    protected TranslogStats stats() throws IOException {
        // force flushing and updating of stats
        translog.sync();
        return translog.stats();
    }

    @Test
    public void testStats() throws IOException {
        // self control cleaning for test
        final long firstOperationPosition = translog.getFirstOperationPosition();
        {
            final TranslogStats stats = stats();
            assertThat(stats.estimatedNumberOfOperations()).isEqualTo(0);
        }
        assertThat(firstOperationPosition).isGreaterThan(CodecUtil.headerLength(TranslogHeader.TRANSLOG_CODEC));
        translog.add(new Translog.Index("1", 0, primaryTerm.get(), new byte[]{1}));

        {
            final TranslogStats stats = stats();
            assertThat(stats.estimatedNumberOfOperations()).isEqualTo(1);
            assertThat(stats.getTranslogSizeInBytes()).isEqualTo(156L);
            assertThat(stats.getUncommittedOperations()).isEqualTo(1);
            assertThat(stats.getUncommittedSizeInBytes()).isEqualTo(101L);
        }

        translog.add(new Translog.Delete("2", 1, primaryTerm.get(), newUid("2")));
        {
            final TranslogStats stats = stats();
            assertThat(stats.estimatedNumberOfOperations()).isEqualTo(2);
            assertThat(stats.getTranslogSizeInBytes()).isEqualTo(199L);
            assertThat(stats.getUncommittedOperations()).isEqualTo(2);
            assertThat(stats.getUncommittedSizeInBytes()).isEqualTo(144L);
        }

        translog.add(new Translog.Delete("3", 2, primaryTerm.get(), newUid("3")));
        {
            final TranslogStats stats = stats();
            assertThat(stats.estimatedNumberOfOperations()).isEqualTo(3);
            assertThat(stats.getTranslogSizeInBytes()).isEqualTo(242L);
            assertThat(stats.getUncommittedOperations()).isEqualTo(3);
            assertThat(stats.getUncommittedSizeInBytes()).isEqualTo(187L);
        }

        translog.add(new Translog.NoOp(3, 1, randomAlphaOfLength(16)));
        {
            final TranslogStats stats = stats();
            assertThat(stats.estimatedNumberOfOperations()).isEqualTo(4);
            assertThat(stats.getTranslogSizeInBytes()).isEqualTo(284L);
            assertThat(stats.getUncommittedOperations()).isEqualTo(4);
            assertThat(stats.getUncommittedSizeInBytes()).isEqualTo(229L);
        }

        translog.rollGeneration();
        {
            final TranslogStats stats = stats();
            assertThat(stats.estimatedNumberOfOperations()).isEqualTo(4);
            assertThat(stats.getTranslogSizeInBytes()).isEqualTo(339L);
            assertThat(stats.getUncommittedOperations()).isEqualTo(4);
            assertThat(stats.getUncommittedSizeInBytes()).isEqualTo(284L);
        }

        {
            final TranslogStats stats = stats();
            assertThat(stats.estimatedNumberOfOperations()).isEqualTo(4);
            assertThat(stats.getTranslogSizeInBytes()).isEqualTo(339L);
        }
        translog.getDeletionPolicy().setLocalCheckpointOfSafeCommit(randomLongBetween(3, Long.MAX_VALUE));
        translog.trimUnreferencedReaders();
        {
            final TranslogStats stats = stats();
            assertThat(stats.estimatedNumberOfOperations()).isEqualTo(0);
            assertThat(stats.getTranslogSizeInBytes()).isEqualTo(firstOperationPosition);
            assertThat(stats.getUncommittedOperations()).isEqualTo(0);
            assertThat(stats.getUncommittedSizeInBytes()).isEqualTo(firstOperationPosition);
        }
    }

    @Test
    public void testUncommittedOperations() throws Exception {
        final TranslogDeletionPolicy deletionPolicy = translog.getDeletionPolicy();
        deletionPolicy.setRetentionAgeInMillis(randomLong());
        deletionPolicy.setRetentionSizeInBytes(randomLong());

        final int operations = scaledRandomIntBetween(10, 100);
        int uncommittedOps = 0;
        int operationsInLastGen = 0;
        for (int i = 0; i < operations; i++) {
            translog.add(new Translog.Index(Integer.toString(i), i, primaryTerm.get(), new byte[]{1}));
            uncommittedOps++;
            operationsInLastGen++;
            if (rarely()) {
                translog.rollGeneration();
                operationsInLastGen = 0;
            }
            assertThat(translog.stats().getUncommittedOperations()).isEqualTo(uncommittedOps);
            if (frequently()) {
                deletionPolicy.setLocalCheckpointOfSafeCommit(i);
                assertThat(translog.stats().getUncommittedOperations()).isEqualTo(operationsInLastGen);
                uncommittedOps = operationsInLastGen;
            }
        }
    }

    @Test
    public void testNegativeNumberOfOperations() {
        assertThatThrownBy(() -> new TranslogStats(-1, 1, 1, 1))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("numberOfOperations must be >= 0");
        assertThatThrownBy(() -> new TranslogStats(1, 1, -1, 1))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("uncommittedOperations must be >= 0");
    }

    @Test
    public void testNegativeSizeInBytes() {
        assertThatThrownBy(() -> new TranslogStats(1, -1, 1, 1))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("translogSizeInBytes must be >= 0");
        assertThatThrownBy(() -> new TranslogStats(1, 1, 1, -1))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("uncommittedSizeInBytes must be >= 0");
    }

    @Test
    public void testBasicSnapshot() throws IOException {
        ArrayList<Translog.Operation> ops = new ArrayList<>();
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
           assertThat(snapshot).hasSize(0);
        }

        addToTranslogAndList(translog, ops, new Translog.Index("1", 0, primaryTerm.get(), new byte[]{1}));

        try (Translog.Snapshot snapshot = translog.newSnapshot(0, Long.MAX_VALUE)) {
            assertThat(snapshot).equalsTo(ops);
            assertThat(snapshot.totalOperations()).isEqualTo(1);
        }

        try (Translog.Snapshot snapshot = translog.newSnapshot(0, randomIntBetween(0, 10));
             Translog.Snapshot snapshot1 = translog.newSnapshot(0, randomIntBetween(0, 10))) {
            assertThat(snapshot).equalsTo(ops);
            assertThat(snapshot.totalOperations()).isEqualTo(1);

           assertThat(snapshot1).hasSize(1);
            assertThat(snapshot1.totalOperations()).isEqualTo(1);
        }
    }

    @Test
    public void testReadLocation() throws IOException {
        ArrayList<Translog.Operation> ops = new ArrayList<>();
        ArrayList<Translog.Location> locs = new ArrayList<>();
        locs.add(addToTranslogAndList(translog, ops, new Translog.Index("1", 0, primaryTerm.get(), new byte[]{1})));
        locs.add(addToTranslogAndList(translog, ops, new Translog.Index("2", 1, primaryTerm.get(), new byte[]{1})));
        locs.add(addToTranslogAndList(translog, ops, new Translog.Index("3", 2, primaryTerm.get(), new byte[]{1})));
        int i = 0;
        for (Translog.Operation op : ops) {
            assertThat(translog.readOperation(locs.get(i++))).isEqualTo(op);
        }
        assertThat(translog.readOperation(new Location(100, 0, 0))).isNull();
    }

    @Test
    public void testSnapshotWithNewTranslog() throws IOException {
        List<Closeable> toClose = new ArrayList<>();
        try {
            ArrayList<Translog.Operation> ops = new ArrayList<>();
            Translog.Snapshot snapshot = translog.newSnapshot();
            toClose.add(snapshot);
            assertThat(snapshot).hasSize(0);

            addToTranslogAndList(translog, ops, new Translog.Index("1", 0, primaryTerm.get(), new byte[]{1}));
            Translog.Snapshot snapshot1 = translog.newSnapshot();
            toClose.add(snapshot1);

            addToTranslogAndList(translog, ops, new Translog.Index("2", 1, primaryTerm.get(), new byte[]{2}));

            assertThat(snapshot1).equalsTo(ops.getFirst());

            translog.rollGeneration();
            addToTranslogAndList(translog, ops, new Translog.Index("3", 2, primaryTerm.get(), new byte[]{3}));

            Translog.Snapshot snapshot2 = translog.newSnapshot();
            toClose.add(snapshot2);
            translog.getDeletionPolicy().setLocalCheckpointOfSafeCommit(2);
            assertThat(snapshot2).containsOperationsInAnyOrder(ops);
            assertThat(snapshot2.totalOperations()).isEqualTo(ops.size());
        } finally {
            IOUtils.closeWhileHandlingException(toClose);
        }
    }

    @Test
    public void testSnapshotOnClosedTranslog() throws IOException {
        assertThat(Files.exists(translogDir.resolve(Translog.getFilename(1)))).isTrue();
        translog.add(new Translog.Index("1", 0, primaryTerm.get(), new byte[]{1}));
        translog.close();
        assertThatThrownBy(() -> translog.newSnapshot())
            .isExactlyInstanceOf(AlreadyClosedException.class)
            .hasMessage("translog is already closed");
    }

    @Test
    public void testRangeSnapshot() throws Exception {
        long minSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        final int generations = between(2, 20);
        Map<Long, List<Translog.Operation>> operationsByGen = new HashMap<>();
        for (int gen = 0; gen < generations; gen++) {
            Set<Long> seqNos = new HashSet<>();
            int numOps = randomIntBetween(1, 100);
            for (int i = 0; i < numOps; i++) {
                final long seqNo = randomValueOtherThanMany(seqNos::contains, () -> randomLongBetween(0, 1000));
                minSeqNo = SequenceNumbers.min(minSeqNo, seqNo);
                maxSeqNo = SequenceNumbers.max(maxSeqNo, seqNo);
                seqNos.add(seqNo);
            }
            List<Translog.Operation> ops = new ArrayList<>(seqNos.size());
            for (long seqNo : seqNos) {
                Translog.Index op = new Translog.Index(randomAlphaOfLength(10), seqNo, primaryTerm.get(), new byte[]{randomByte()});
                translog.add(op);
                ops.add(op);
            }
            operationsByGen.put(translog.currentFileGeneration(), ops);
            translog.rollGeneration();
            if (rarely()) {
                translog.rollGeneration(); // empty generation
            }
        }

        if (minSeqNo > 0) {
            long fromSeqNo = randomLongBetween(0, minSeqNo - 1);
            long toSeqNo = randomLongBetween(fromSeqNo, minSeqNo - 1);
            try (Translog.Snapshot snapshot = translog.newSnapshot(fromSeqNo, toSeqNo)) {
                assertThat(snapshot.totalOperations()).isEqualTo(0);
                assertThat(snapshot.next()).isNull();
            }
        }

        long fromSeqNo = randomLongBetween(maxSeqNo + 1, Long.MAX_VALUE);
        long toSeqNo = randomLongBetween(fromSeqNo, Long.MAX_VALUE);
        try (Translog.Snapshot snapshot = translog.newSnapshot(fromSeqNo, toSeqNo)) {
            assertThat(snapshot.totalOperations()).isEqualTo(0);
            assertThat(snapshot.next()).isNull();
        }

        fromSeqNo = randomLongBetween(0, 2000);
        toSeqNo = randomLongBetween(fromSeqNo, 2000);
        try (Translog.Snapshot snapshot = translog.newSnapshot(fromSeqNo, toSeqNo)) {
            Set<Long> seenSeqNos = new HashSet<>();
            List<Translog.Operation> expectedOps = new ArrayList<>();
            for (long gen = translog.currentFileGeneration(); gen > 0; gen--) {
                for (Translog.Operation op : operationsByGen.getOrDefault(gen, Collections.emptyList())) {
                    if (fromSeqNo <= op.seqNo() && op.seqNo() <= toSeqNo && seenSeqNos.add(op.seqNo())) {
                        expectedOps.add(op);
                    }
                }
            }
            assertThat(TestTranslog.drainSnapshot(snapshot, false)).isEqualTo(expectedOps);
        }
    }

    public void assertFileIsPresent(Translog translog, long id) {
        if (Files.exists(translog.location().resolve(Translog.getFilename(id)))) {
            return;
        }
        fail(Translog.getFilename(id) + " is not present in any location: " + translog.location());
    }

    public void assertFileDeleted(Translog translog, long id) {
        assertThat(Files.exists(translog.location().resolve(Translog.getFilename(id)))).as("translog [" + id + "] still exists").isFalse();
    }

    private void assertFilePresences(Translog translog) {
        for (long gen = translog.getMinFileGeneration(); gen < translog.currentFileGeneration(); gen++) {
            assertFileIsPresent(translog, gen);
        }
        for (long gen = 1; gen < translog.getMinFileGeneration(); gen++) {
            assertFileDeleted(translog, gen);
        }

    }

    static class LocationOperation implements Comparable<LocationOperation> {
        final Translog.Operation operation;
        final Translog.Location location;

        LocationOperation(Translog.Operation operation, Translog.Location location) {
            this.operation = operation;
            this.location = location;
        }

        @Override
        public int compareTo(LocationOperation o) {
            return location.compareTo(o.location);
        }
    }

    @Test
    public void testConcurrentWritesWithVaryingSize() throws Throwable {
        final int opsPerThread = randomIntBetween(10, 200);
        int threadCount = 2 + randomInt(5);

        logger.info("testing with [{}] threads, each doing [{}] ops", threadCount, opsPerThread);
        final BlockingQueue<LocationOperation> writtenOperations = new ArrayBlockingQueue<>(threadCount * opsPerThread);

        Thread[] threads = new Thread[threadCount];
        final Exception[] threadExceptions = new Exception[threadCount];
        final AtomicLong seqNoGenerator = new AtomicLong();
        final CountDownLatch downLatch = new CountDownLatch(1);
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            threads[i] =
                new TranslogThread(translog, downLatch, opsPerThread, threadId, writtenOperations, seqNoGenerator, threadExceptions);
            threads[i].setDaemon(true);
            threads[i].start();
        }

        downLatch.countDown();

        for (int i = 0; i < threadCount; i++) {
            if (threadExceptions[i] != null) {
                throw threadExceptions[i];
            }
            threads[i].join(60 * 1000);
        }

        List<LocationOperation> collect = new ArrayList<>(writtenOperations);
        Collections.sort(collect);
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            for (LocationOperation locationOperation : collect) {
                Translog.Operation op = snapshot.next();
                assertThat(op).isNotNull();
                Translog.Operation expectedOp = locationOperation.operation;
                if (randomBoolean()) {
                    assertThat(translog.readOperation(locationOperation.location)).isEqualTo(expectedOp);
                }
                assertThat(op.opType()).isEqualTo(expectedOp.opType());
                switch (op.opType()) {
                    case INDEX:
                        Translog.Index indexOp = (Translog.Index) op;
                        Translog.Index expIndexOp = (Translog.Index) expectedOp;
                        assertThat(indexOp.id()).isEqualTo(expIndexOp.id());
                        assertThat(indexOp.getSource()).isEqualTo(expIndexOp.getSource());
                        assertThat(indexOp.version()).isEqualTo(expIndexOp.version());
                        break;
                    case DELETE:
                        Translog.Delete delOp = (Translog.Delete) op;
                        Translog.Delete expDelOp = (Translog.Delete) expectedOp;
                        assertThat(delOp.uid()).isEqualTo(expDelOp.uid());
                        assertThat(delOp.version()).isEqualTo(expDelOp.version());
                        break;
                    case NO_OP:
                        final Translog.NoOp noOp = (Translog.NoOp) op;
                        final Translog.NoOp expectedNoOp = (Translog.NoOp) expectedOp;
                        assertThat(noOp.seqNo()).isEqualTo(expectedNoOp.seqNo());
                        assertThat(noOp.primaryTerm()).isEqualTo(expectedNoOp.primaryTerm());
                        assertThat(noOp.reason()).isEqualTo(expectedNoOp.reason());
                        break;
                    default:
                        throw new AssertionError("unsupported operation type [" + op.opType() + "]");
                }
            }
            assertThat(snapshot.next()).isNull();
        }

    }

    @Test
    public void testTranslogCorruption() throws Exception {
        TranslogConfig config = translog.getConfig();
        String uuid = translog.getTranslogUUID();
        List<Translog.Location> locations = new ArrayList<>();

        int translogOperations = randomIntBetween(10, 1000);
        for (int op = 0; op < translogOperations; op++) {
            String ascii = randomAlphaOfLengthBetween(1, 50);
            locations.add(
                translog.add(new Translog.Index("" + op, op, primaryTerm.get(), ascii.getBytes("UTF-8")))
            );

            if (rarely()) {
                translog.rollGeneration();
            }
        }
        translog.close();

        TestTranslog.corruptRandomTranslogFile(logger, random(), translogDir, 0);

        assertThatThrownBy(() -> {
            try (Translog translog = openTranslog(config, uuid);
                 Translog.Snapshot snapshot = translog.newSnapshot()) {
                for (int i = 0; i < locations.size(); i++) {
                    snapshot.next();
                }
            }
        }).isExactlyInstanceOf(TranslogCorruptedException.class)
            .hasMessageContaining(translogDir.toString());

        expectIntactTranslog = false;
    }

    @Test
    public void testTruncatedTranslogs() throws Exception {
        List<Translog.Location> locations = new ArrayList<>();

        int translogOperations = randomIntBetween(10, 100);
        for (int op = 0; op < translogOperations; op++) {
            String ascii = randomAlphaOfLengthBetween(1, 50);
            locations.add(translog.add(
                new Translog.Index("" + op, op, primaryTerm.get(), ascii.getBytes("UTF-8")))
            );
        }
        translog.sync();

        truncateTranslogs(translogDir);

        AtomicInteger truncations = new AtomicInteger(0);
        try (Translog.Snapshot snap = translog.newSnapshot()) {
            for (int i = 0; i < locations.size(); i++) {
                try {
                    assertThat(snap.next()).isNotNull();
                } catch (TranslogCorruptedException e) {
                    assertThat(e.getCause()).isExactlyInstanceOf(EOFException.class);
                    truncations.incrementAndGet();
                }
            }
        }
        assertThat(truncations.get())
            .as("at least one truncation was caused and caught")
            .isGreaterThanOrEqualTo(1);
    }

    /**
     * Randomly truncate some bytes in the translog files
     */
    private void truncateTranslogs(Path directory) throws Exception {
        Path[] files = FileSystemUtils.files(directory, "translog-*");
        for (Path file : files) {
            try (FileChannel f = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                long prevSize = f.size();
                long newSize = prevSize - randomIntBetween(1, (int) prevSize / 2);
                logger.info("--> truncating {}, prev: {}, now: {}", file, prevSize, newSize);
                f.truncate(newSize);
            }
        }
    }


    private Term newUid(ParsedDocument doc) {
        return new Term("_id", Uid.encodeId(doc.id()));
    }

    private Term newUid(String id) {
        return new Term("_id", Uid.encodeId(id));
    }

    @Test
    public void testVerifyTranslogIsNotDeleted() throws IOException {
        assertFileIsPresent(translog, 1);
        translog.add(new Translog.Index("1", 0, primaryTerm.get(), new byte[]{1}));
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
           assertThat(snapshot).hasSize(1);
            assertFileIsPresent(translog, 1);
            assertThat(snapshot.totalOperations()).isEqualTo(1);
        }
        translog.close();
        assertFileDeleted(translog, 1);
        assertFileIsPresent(translog, 2);
    }

    /**
     * Tests that concurrent readers and writes maintain view and snapshot semantics
     */
    @Test
    public void testConcurrentWriteViewsAndSnapshot() throws Throwable {
        final Thread[] writers = new Thread[randomIntBetween(1, 3)];
        final Thread[] readers = new Thread[randomIntBetween(1, 3)];
        final int flushEveryOps = randomIntBetween(5, 100);
        final int maxOps = randomIntBetween(200, 1000);
        final Object signalReaderSomeDataWasIndexed = new Object();
        final AtomicLong idGenerator = new AtomicLong();
        final CyclicBarrier barrier = new CyclicBarrier(writers.length + readers.length + 1);

        // a map of all written ops and their returned location.
        final Map<Translog.Operation, Translog.Location> writtenOps = new ConcurrentHashMap<>();

        // a signal for all threads to stop
        final AtomicBoolean run = new AtomicBoolean(true);

        final Object flushMutex = new Object();
        final AtomicLong lastCommittedLocalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        final LocalCheckpointTracker tracker = LocalCheckpointTrackerTests.createEmptyTracker();
        final TranslogDeletionPolicy deletionPolicy = translog.getDeletionPolicy();
        // any errors on threads
        final List<Exception> errors = new CopyOnWriteArrayList<>();
        logger.info("using [{}] readers. [{}] writers. flushing every ~[{}] ops.", readers.length, writers.length, flushEveryOps);
        for (int i = 0; i < writers.length; i++) {
            final String threadName = "writer_" + i;
            final int threadId = i;
            writers[i] = new Thread(new RejectableRunnable() {
                @Override
                public void doRun() throws BrokenBarrierException, InterruptedException, IOException {
                    barrier.await();
                    int counter = 0;
                    while (run.get() && idGenerator.get() < maxOps) {
                        long id = idGenerator.getAndIncrement();
                        final Translog.Operation op;
                        final Translog.Operation.Type type =
                            Translog.Operation.Type.values()[((int) (id % Translog.Operation.Type.values().length))];
                        switch (type) {
                            case CREATE:
                            case INDEX:
                                op = new Translog.Index("" + id, id, primaryTerm.get(), new byte[]{(byte) id});
                                break;
                            case DELETE:
                                op = new Translog.Delete(Long.toString(id), id, primaryTerm.get(), newUid(Long.toString(id)));
                                break;
                            case NO_OP:
                                op = new Translog.NoOp(id, 1, Long.toString(id));
                                break;
                            default:
                                throw new AssertionError("unsupported operation type [" + type + "]");
                        }
                        Translog.Location location = translog.add(op);
                        tracker.markSeqNoAsProcessed(id);
                        Translog.Location existing = writtenOps.put(op, location);
                        if (existing != null) {
                            fail("duplicate op [" + op + "], old entry at " + location);
                        }
                        if (id % writers.length == threadId) {
                            translog.ensureSynced(location);
                        }
                        if (id % flushEveryOps == 0) {
                            synchronized (flushMutex) {
                                // we need not do this concurrently as we need to make sure that the generation
                                // we're committing - is still present when we're committing
                                long localCheckpoint = tracker.getProcessedCheckpoint();
                                translog.rollGeneration();
                                // expose the new checkpoint (simulating a commit), before we trim the translog
                                lastCommittedLocalCheckpoint.set(localCheckpoint);
                                deletionPolicy.setLocalCheckpointOfSafeCommit(localCheckpoint);
                                translog.trimUnreferencedReaders();
                            }
                        }
                        if (id % 7 == 0) {
                            synchronized (signalReaderSomeDataWasIndexed) {
                                signalReaderSomeDataWasIndexed.notifyAll();
                            }
                        }
                        counter++;
                    }
                    logger.info("--> [{}] done. wrote [{}] ops.", threadName, counter);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error(() -> new ParameterizedMessage("--> writer [{}] had an error", threadName), e);
                    errors.add(e);
                }
            }, threadName);
            writers[i].start();
        }

        for (int i = 0; i < readers.length; i++) {
            final String threadId = "reader_" + i;
            readers[i] = new Thread(new RejectableRunnable() {
                Closeable retentionLock = null;
                long committedLocalCheckpointAtView;

                @Override
                public void onFailure(Exception e) {
                    logger.error(() -> new ParameterizedMessage("--> reader [{}] had an error", threadId), e);
                    errors.add(e);
                    try {
                        closeRetentionLock();
                    } catch (IOException inner) {
                        inner.addSuppressed(e);
                        logger.error("unexpected error while closing view, after failure", inner);
                    }
                }

                void closeRetentionLock() throws IOException {
                    if (retentionLock != null) {
                        retentionLock.close();
                    }
                }

                void acquireRetentionLock() throws IOException {
                    closeRetentionLock();
                    retentionLock = translog.acquireRetentionLock();
                    // captures the last committed checkpoint, while holding the view, simulating
                    // recovery logic which captures a view and gets a lucene commit
                    committedLocalCheckpointAtView = lastCommittedLocalCheckpoint.get();
                    logger.info("--> [{}] min gen after acquiring lock [{}]", threadId, translog.getMinFileGeneration());
                }

                @Override
                public void doRun() throws Exception {
                    barrier.await();
                    int iter = 0;
                    while (idGenerator.get() < maxOps) {
                        if (iter++ % 10 == 0) {
                            acquireRetentionLock();
                        }

                        // captures al views that are written since the view was created (with a small caveat see bellow)
                        // these are what we expect the snapshot to return (and potentially some more).
                        Set<Translog.Operation> expectedOps = new HashSet<>(writtenOps.keySet());
                        expectedOps.removeIf(op -> op.seqNo() <= committedLocalCheckpointAtView);
                        try (Translog.Snapshot snapshot = translog.newSnapshot(committedLocalCheckpointAtView + 1L, Long.MAX_VALUE)) {
                            Translog.Operation op;
                            while ((op = snapshot.next()) != null) {
                                expectedOps.remove(op);
                            }
                        }
                        if (expectedOps.isEmpty() == false) {
                            StringBuilder missed = new StringBuilder("missed ").append(expectedOps.size())
                                .append(" operations from [").append(committedLocalCheckpointAtView + 1L).append("]");
                            boolean failed = false;
                            for (Translog.Operation expectedOp : expectedOps) {
                                final Translog.Location loc = writtenOps.get(expectedOp);
                                failed = true;
                                missed.append("\n --> [").append(expectedOp).append("] written at ").append(loc);
                            }
                            if (failed) {
                                fail(missed.toString());
                            }
                        }
                        // slow down things a bit and spread out testing..
                        synchronized (signalReaderSomeDataWasIndexed) {
                            if (idGenerator.get() < maxOps) {
                                signalReaderSomeDataWasIndexed.wait();
                            }
                        }
                    }
                    closeRetentionLock();
                    logger.info("--> [{}] done. tested [{}] snapshots", threadId, iter);
                }
            }, threadId);
            readers[i].start();
        }

        barrier.await();
        logger.debug("--> waiting for threads to stop");
        for (Thread thread : writers) {
            thread.join();
        }
        logger.debug("--> waiting for readers to stop");
        // force stopping, if all writers crashed
        synchronized (signalReaderSomeDataWasIndexed) {
            idGenerator.set(Long.MAX_VALUE);
            signalReaderSomeDataWasIndexed.notifyAll();
        }
        for (Thread thread : readers) {
            thread.join();
        }
        if (errors.size() > 0) {
            Throwable e = errors.get(0);
            for (Throwable suppress : errors.subList(1, errors.size())) {
                e.addSuppressed(suppress);
            }
            throw e;
        }
        logger.info("--> test done. total ops written [{}]", writtenOps.size());
    }

    @Test
    public void testSyncUpTo() throws IOException {
        int translogOperations = randomIntBetween(10, 100);
        int count = 0;
        for (int op = 0; op < translogOperations; op++) {
            int seqNo = ++count;
            final Translog.Location location =
                translog.add(new Translog.Index("" + op, seqNo, primaryTerm.get(),
                    Integer.toString(seqNo).getBytes(Charset.forName("UTF-8"))));
            if (randomBoolean()) {
                assertThat(translog.syncNeeded()).as("at least one operation pending").isTrue();
                assertThat(translog.ensureSynced(location)).as("this operation has not been synced").isTrue();
                // we are the last location so everything should be synced
                assertThat(translog.syncNeeded()).as("the last call to ensureSycned synced all previous ops").isFalse();
                seqNo = ++count;
                translog.add(new Translog.Index("" + op, seqNo, primaryTerm.get(),
                    Integer.toString(seqNo).getBytes(Charset.forName("UTF-8"))));
                assertThat(translog.syncNeeded()).as("one pending operation").isTrue();
                assertThat(translog.ensureSynced(location)).as("this op has been synced before").isFalse(); // not syncing now
                assertThat(translog.syncNeeded()).as("we only synced a previous operation yet").isTrue();
            }
            if (rarely()) {
                translog.rollGeneration();
                assertThat(translog.ensureSynced(location)).as("location is from a previous translog - already synced").isFalse(); // not syncing now
                assertThat(translog.syncNeeded()).as("no sync needed since no operations in current translog").isFalse();
            }

            if (randomBoolean()) {
                translog.sync();
                assertThat(translog.ensureSynced(location)).as("translog has been synced already").isFalse();
            }
        }
    }

    @Test
    public void testSyncUpToStream() throws IOException {
        int iters = randomIntBetween(5, 10);
        for (int i = 0; i < iters; i++) {
            int translogOperations = randomIntBetween(10, 100);
            int count = 0;
            ArrayList<Location> locations = new ArrayList<>();
            for (int op = 0; op < translogOperations; op++) {
                if (rarely()) {
                    translog.rollGeneration();
                }
                final Translog.Location location =
                    translog.add(new Translog.Index("" + op, op, primaryTerm.get(),
                        Integer.toString(++count).getBytes(Charset.forName("UTF-8"))));
                locations.add(location);
            }
            Collections.shuffle(locations, random());
            if (randomBoolean()) {
                assertThat(translog.syncNeeded()).as("at least one operation pending").isTrue();
                assertThat(translog.ensureSynced(locations.stream())).as("this operation has not been synced").isTrue();
                // we are the last location so everything should be synced
                assertThat(translog.syncNeeded()).as("the last call to ensureSycned synced all previous ops").isFalse();
            } else if (rarely()) {
                translog.rollGeneration();
                // not syncing now
                assertThat(translog.ensureSynced(locations.stream())).as("location is from a previous translog - already synced").isFalse();
                assertThat(translog.syncNeeded()).as("no sync needed since no operations in current translog").isFalse();
            } else {
                translog.sync();
                assertThat(translog.ensureSynced(locations.stream())).as("translog has been synced already").isFalse();
            }
            for (Location location : locations) {
                assertThat(translog.ensureSynced(location)).as("all of the locations should be synced: " + location).isFalse();
            }
        }
    }

    @Test
    public void testLocationComparison() throws IOException {
        List<Translog.Location> locations = new ArrayList<>();
        int translogOperations = randomIntBetween(10, 100);
        int count = 0;
        for (int op = 0; op < translogOperations; op++) {
            locations.add(
                translog.add(new Translog.Index("" + op, op,
                    primaryTerm.get(), Integer.toString(++count).getBytes(Charset.forName("UTF-8")))));
            if (rarely() && translogOperations > op + 1) {
                translog.rollGeneration();
            }
        }
        Collections.shuffle(locations, random());
        Translog.Location max = locations.get(0);
        for (Translog.Location location : locations) {
            max = max(max, location);
        }

        assertThat(translog.currentFileGeneration()).isEqualTo(max.generation);
        try (Translog.Snapshot snap = new SortedSnapshot(translog.newSnapshot())) {
            Translog.Operation next;
            Translog.Operation maxOp = null;
            while ((next = snap.next()) != null) {
                maxOp = next;
            }
            assertThat(maxOp).isNotNull();
            assertThat(Integer.toString(count)).isEqualTo(maxOp.getSource().utf8ToString());
        }
    }

    public static Translog.Location max(Translog.Location a, Translog.Location b) {
        if (a.compareTo(b) > 0) {
            return a;
        }
        return b;
    }


    @Test
    public void testBasicCheckpoint() throws IOException {
        List<Translog.Location> locations = new ArrayList<>();
        int translogOperations = randomIntBetween(10, 100);
        int lastSynced = -1;
        long lastSyncedGlobalCheckpoint = globalCheckpoint.get();
        for (int op = 0; op < translogOperations; op++) {
            locations.add(translog.add(new Translog.Index("" + op, op, primaryTerm.get(),
                Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
            if (randomBoolean()) {
                globalCheckpoint.set(globalCheckpoint.get() + randomIntBetween(1, 16));
            }
            if (frequently()) {
                translog.sync();
                lastSynced = op;
                lastSyncedGlobalCheckpoint = globalCheckpoint.get();
            }
        }
        assertThat(translog.totalOperations()).isEqualTo(translogOperations);
        translog.add(new Translog.Index("" + translogOperations, translogOperations, primaryTerm.get(),
            Integer.toString(translogOperations).getBytes(Charset.forName("UTF-8"))));

        final Checkpoint checkpoint = Checkpoint.read(translog.location().resolve(Translog.CHECKPOINT_FILE_NAME));
        try (TranslogReader reader =
                 translog.openReader(translog.location().resolve(Translog.getFilename(translog.currentFileGeneration())), checkpoint)) {
            assertThat(reader.totalOperations()).isEqualTo(lastSynced + 1);
            TranslogSnapshot snapshot = reader.newSnapshot();

            for (int op = 0; op < translogOperations; op++) {
                if (op <= lastSynced) {
                    final Translog.Operation read = snapshot.next();
                    assertThat(read.getSource().utf8ToString()).isEqualTo(Integer.toString(op));
                } else {
                    Translog.Operation next = snapshot.next();
                    assertThat(next).isNull();
                }
            }
            Translog.Operation next = snapshot.next();
            assertThat(next).isNull();
        }
        assertThat(translog.totalOperations()).isEqualTo(translogOperations + 1);
        assertThat(checkpoint.globalCheckpoint).isEqualTo(lastSyncedGlobalCheckpoint);
        translog.close();
    }

    @Test
    public void testTranslogWriter() throws IOException {
        final TranslogWriter writer = translog.createWriter(translog.currentFileGeneration() + 1);
        final Set<Long> persistedSeqNos = new HashSet<>();
        persistedSeqNoConsumer.set(persistedSeqNos::add);
        final int numOps = scaledRandomIntBetween(8, 250000);
        final Set<Long> seenSeqNos = new HashSet<>();
        boolean opsHaveValidSequenceNumbers = randomBoolean();
        for (int i = 0; i < numOps; i++) {
            byte[] bytes = new byte[4];
            DataOutput out = EndiannessReverserUtil.wrapDataOutput(new ByteArrayDataOutput(bytes));
            out.writeInt(i);
            long seqNo;
            do {
                seqNo = opsHaveValidSequenceNumbers ? randomNonNegativeLong() : SequenceNumbers.UNASSIGNED_SEQ_NO;
                opsHaveValidSequenceNumbers = opsHaveValidSequenceNumbers || !rarely();
            } while (seenSeqNos.contains(seqNo));
            if (seqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                seenSeqNos.add(seqNo);
            }
            writer.add(ReleasableBytesReference.wrap(new BytesArray(bytes)), seqNo);
        }
        assertThat(persistedSeqNos).isEmpty();
        writer.sync();
        persistedSeqNos.remove(SequenceNumbers.UNASSIGNED_SEQ_NO);
        assertThat(persistedSeqNos).isEqualTo(seenSeqNos);

        final BaseTranslogReader reader = randomBoolean()
            ? writer
            : translog.openReader(writer.path(), Checkpoint.read(translog.location().resolve(Translog.CHECKPOINT_FILE_NAME)));
        for (int i = 0; i < numOps; i++) {
            ByteBuffer buffer = ByteBuffer.allocate(4);
            reader.readBytes(buffer, reader.getFirstOperationOffset() + 4 * i);
            buffer.flip();
            final int value = buffer.getInt();
            assertThat(value).isEqualTo(i);
        }
        final long minSeqNo = seenSeqNos.stream().min(Long::compareTo).orElse(SequenceNumbers.NO_OPS_PERFORMED);
        final long maxSeqNo = seenSeqNos.stream().max(Long::compareTo).orElse(SequenceNumbers.NO_OPS_PERFORMED);
        assertThat(reader.getCheckpoint().minSeqNo).isEqualTo(minSeqNo);
        assertThat(reader.getCheckpoint().maxSeqNo).isEqualTo(maxSeqNo);

        byte[] bytes = new byte[4];
        DataOutput out = EndiannessReverserUtil.wrapDataOutput(new ByteArrayDataOutput(bytes));
        out.writeInt(2048);
        writer.add(ReleasableBytesReference.wrap(new BytesArray(bytes)), randomNonNegativeLong());

        if (reader instanceof TranslogReader) {
            ByteBuffer buffer = ByteBuffer.allocate(4);
            try {
                reader.readBytes(buffer, reader.getFirstOperationOffset() + 4 * numOps);
                fail("read past EOF?");
            } catch (EOFException ex) {
                // expected
            }
            ((TranslogReader) reader).close();
        } else {
            // live reader!
            ByteBuffer buffer = ByteBuffer.allocate(4);
            final long pos = reader.getFirstOperationOffset() + 4 * numOps;
            reader.readBytes(buffer, pos);
            buffer.flip();
            final int value = buffer.getInt();
            assertThat(value).isEqualTo(2048);
        }
        IOUtils.close(writer);
    }

    @Test
    public void testTranslogWriterCanFlushInAddOrReadCall() throws IOException {
        Path tempDir = createTempDir();
        final TranslogConfig temp = getTranslogConfig(tempDir);
        final TranslogConfig config = new TranslogConfig(temp.getShardId(), temp.getTranslogPath(), temp.getIndexSettings(),
            temp.getBigArrays(), new ByteSizeValue(1, ByteSizeUnit.KB));

        final Set<Long> persistedSeqNos = new HashSet<>();
        final AtomicInteger writeCalls = new AtomicInteger();

        final ChannelFactory channelFactory = (file, openOption) -> {
            FileChannel delegate = FileChannel.open(file, openOption);
            boolean success = false;
            try {
                // don't do partial writes for checkpoints we rely on the fact that the bytes are written as an atomic operation
                final boolean isCkpFile = file.getFileName().toString().endsWith(".ckp");

                final FileChannel channel;
                if (isCkpFile) {
                    channel = delegate;
                } else {
                    channel = new FilterFileChannel(delegate) {

                        @Override
                        public int write(ByteBuffer src) throws IOException {
                            writeCalls.incrementAndGet();
                            return super.write(src);
                        }
                    };
                }
                success = true;
                return channel;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(delegate);
                }
            }
        };

        String translogUUID = Translog.createEmptyTranslog(
            config.getTranslogPath(), SequenceNumbers.NO_OPS_PERFORMED, shardId, channelFactory, primaryTerm.get());

        try (Translog translog = new Translog(config, translogUUID, new TranslogDeletionPolicy(-1, -1, 0),
            () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTerm::get, persistedSeqNos::add) {
            @Override
            ChannelFactory getChannelFactory() {
                return channelFactory;
            }
        }) {
            TranslogWriter writer = translog.getCurrent();
            int initialWriteCalls = writeCalls.get();
            byte[] bytes = new byte[256];
            writer.add(ReleasableBytesReference.wrap(new BytesArray(bytes)), 1);
            writer.add(ReleasableBytesReference.wrap(new BytesArray(bytes)), 2);
            writer.add(ReleasableBytesReference.wrap(new BytesArray(bytes)), 3);
            writer.add(ReleasableBytesReference.wrap(new BytesArray(bytes)), 4);
            assertThat(persistedSeqNos).isEmpty();
            assertThat(writeCalls.get()).isEqualTo(initialWriteCalls);

            if (randomBoolean()) {
                // Since the buffer is full, this will flush before performing the add.
                writer.add(ReleasableBytesReference.wrap(new BytesArray(bytes)), 5);
                assertThat(persistedSeqNos).isEmpty();
                assertThat(writeCalls.get()).isGreaterThan(initialWriteCalls);
            } else {
                // Will flush on read
                writer.readBytes(ByteBuffer.allocate(256), 0);
                assertThat(persistedSeqNos).isEmpty();
                assertThat(writeCalls.get()).isGreaterThan(initialWriteCalls);

                // Add after we the read flushed the buffer
                writer.add(ReleasableBytesReference.wrap(new BytesArray(bytes)), 5);
            }

            writer.sync();

            // Sequence numbers are marked as persisted after sync
            assertThat(persistedSeqNos).containsExactly(1L, 2L, 3L, 4L, 5L);
        }
    }

    @Test
    public void testTranslogWriterDoesNotBlockAddsOnWrite() throws IOException, InterruptedException {
        Path tempDir = createTempDir();
        final TranslogConfig config = getTranslogConfig(tempDir);
        final AtomicBoolean startBlocking = new AtomicBoolean(false);
        final CountDownLatch writeStarted = new CountDownLatch(1);
        final CountDownLatch blocker = new CountDownLatch(1);
        final Set<Long> persistedSeqNos = new HashSet<>();

        final ChannelFactory channelFactory = (file, openOption) -> {
            FileChannel delegate = FileChannel.open(file, openOption);
            boolean success = false;
            try {
                // don't do partial writes for checkpoints we rely on the fact that the bytes are written as an atomic operation
                final boolean isCkpFile = file.getFileName().toString().endsWith(".ckp");

                final FileChannel channel;
                if (isCkpFile) {
                    channel = delegate;
                } else {
                    channel = new FilterFileChannel(delegate) {

                        @Override
                        public int write(ByteBuffer src) throws IOException {
                            if (startBlocking.get()) {
                                if (writeStarted.getCount() > 0) {
                                    writeStarted.countDown();
                                }
                                try {
                                    blocker.await();
                                } catch (InterruptedException e) {
                                    // Ignore
                                }
                            }
                            return super.write(src);
                        }

                        @Override
                        public void force(boolean metaData) throws IOException {
                            if (startBlocking.get()) {
                                if (writeStarted.getCount() > 0) {
                                    writeStarted.countDown();
                                }
                                try {
                                    blocker.await();
                                } catch (InterruptedException e) {
                                    // Ignore
                                }
                            }
                            super.force(metaData);
                        }
                    };
                }
                success = true;
                return channel;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(delegate);
                }
            }
        };
        String translogUUID = Translog.createEmptyTranslog(
            config.getTranslogPath(), SequenceNumbers.NO_OPS_PERFORMED, shardId, channelFactory, primaryTerm.get());

        try (Translog translog = new Translog(config, translogUUID, new TranslogDeletionPolicy(-1, -1, 0),
            () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTerm::get, persistedSeqNos::add) {
            @Override
            ChannelFactory getChannelFactory() {
                return channelFactory;
            }
        }) {
            TranslogWriter writer = translog.getCurrent();

            byte[] bytes = new byte[4];
            ByteArrayDataOutput out = new ByteArrayDataOutput(new byte[4]);
            out.writeInt(1);
            writer.add(ReleasableBytesReference.wrap(new BytesArray(bytes)), 1);
            assertThat(persistedSeqNos).isEmpty();
            startBlocking.set(true);
            Thread thread = new Thread(() -> {
                try {
                    writer.sync();
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            });
            thread.start();
            writeStarted.await();

            // Add will not block even though we are currently writing/syncing
            writer.add(ReleasableBytesReference.wrap(new BytesArray(bytes)), 2);

            blocker.countDown();
            // Sync against so that both operations are written
            writer.sync();

            assertThat(persistedSeqNos).containsExactly(1L, 2L);
            thread.join();
        }
    }

    @Test
    public void testCloseIntoReader() throws IOException {
        try (TranslogWriter writer = translog.createWriter(translog.currentFileGeneration() + 1)) {
            final int numOps = randomIntBetween(8, 128);
            for (int i = 0; i < numOps; i++) {
                final byte[] bytes = new byte[4];
                final DataOutput out = EndiannessReverserUtil.wrapDataOutput(new ByteArrayDataOutput(bytes));
                out.writeInt(i);
                writer.add(ReleasableBytesReference.wrap(new BytesArray(bytes)), randomNonNegativeLong());
            }
            writer.sync();
            final Checkpoint writerCheckpoint = writer.getCheckpoint();
            TranslogReader reader = writer.closeIntoReader();
            try {
                if (randomBoolean()) {
                    reader.close();
                    reader = translog.openReader(reader.path(), writerCheckpoint);
                }
                for (int i = 0; i < numOps; i++) {
                    final ByteBuffer buffer = ByteBuffer.allocate(4);
                    reader.readBytes(buffer, reader.getFirstOperationOffset() + 4 * i);
                    buffer.flip();
                    final int value = buffer.getInt();
                    assertThat(value).isEqualTo(i);
                }
                final Checkpoint readerCheckpoint = reader.getCheckpoint();
                assertThat(readerCheckpoint).isEqualTo(writerCheckpoint);
            } finally {
                IOUtils.close(reader);
            }
        }
    }

    @Test
    public void testBasicRecovery() throws IOException {
        List<Translog.Location> locations = new ArrayList<>();
        int translogOperations = randomIntBetween(10, 100);
        Translog.TranslogGeneration translogGeneration = null;
        int minUncommittedOp = -1;
        final boolean commitOften = randomBoolean();
        for (int op = 0; op < translogOperations; op++) {
            locations.add(translog.add(new Translog.Index("" + op, op, primaryTerm.get(),
                Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
            final boolean commit = commitOften ? frequently() : rarely();
            if (commit && op < translogOperations - 1) {
                translog.getDeletionPolicy().setLocalCheckpointOfSafeCommit(op);
                minUncommittedOp = op + 1;
                translogGeneration = translog.getGeneration();
            }
        }
        translog.sync();
        TranslogConfig config = translog.getConfig();

        translog.close();
        if (translogGeneration == null) {
            translog = createTranslog(config);
            assertThat(translog.stats().estimatedNumberOfOperations()).isEqualTo(0);
            assertThat(translog.currentFileGeneration()).isEqualTo(2);
            assertThat(translog.syncNeeded()).isFalse();
            try(Translog.Snapshot snapshot = translog.newSnapshot()) {
                assertThat(snapshot.next()).isNull();
            }
        } else {
            translog = new Translog(config, translogGeneration.translogUUID, translog.getDeletionPolicy(),
                () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTerm::get, seqNo -> {});
            assertThat(translog.currentFileGeneration()).as("lastCommitted must be 1 less than current").isEqualTo(translogGeneration.translogFileGeneration + 1);
            assertThat(translog.syncNeeded()).isFalse();
            try (Translog.Snapshot snapshot = translog.newSnapshot(minUncommittedOp, Long.MAX_VALUE)) {
                for (int i = minUncommittedOp; i < translogOperations; i++) {
                    assertThat(locations.get(i).generation).as("expected operation" + i + " to be in the previous translog but wasn't").isEqualTo(translog.currentFileGeneration() - 1);
                    Translog.Operation next = snapshot.next();
                    assertThat(next).as("operation " + i + " must be non-null").isNotNull();
                    assertThat(Integer.parseInt(next.getSource().utf8ToString())).isEqualTo(i);
                }
            }
        }
    }

    @Test
    public void testRecoveryUncommitted() throws IOException {
        List<Translog.Location> locations = new ArrayList<>();
        int translogOperations = randomIntBetween(10, 100);
        final int prepareOp = randomIntBetween(0, translogOperations - 1);
        Translog.TranslogGeneration translogGeneration = null;
        final boolean sync = randomBoolean();
        for (int op = 0; op < translogOperations; op++) {
            locations.add(translog.add(new Translog.Index("" + op, op, primaryTerm.get(),
                Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
            if (op == prepareOp) {
                translogGeneration = translog.getGeneration();
                translog.rollGeneration();
                assertThat(translogGeneration.translogFileGeneration).as("expected this to be the first roll (1 gen is on creation, 2 when opened)").isEqualTo(2L);
                assertThat(translogGeneration.translogUUID).isNotNull();
            }
        }
        if (sync) {
            translog.sync();
        }
        // we intentionally don't close the tlog that is in the prepareCommit stage since we try to recovery the uncommitted
        // translog here as well.
        TranslogConfig config = translog.getConfig();
        final String translogUUID = translog.getTranslogUUID();
        final TranslogDeletionPolicy deletionPolicy = translog.getDeletionPolicy();
        try (Translog translog = new Translog(config, translogUUID, deletionPolicy,
                () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTerm::get, seqNo -> {})) {
            assertThat(translogGeneration).isNotNull();
            assertThat(translog.currentFileGeneration()).as("lastCommitted must be 2 less than current - we never finished the commit").isEqualTo(translogGeneration.translogFileGeneration + 2);
            assertThat(translog.syncNeeded()).isFalse();
            try (Translog.Snapshot snapshot = new SortedSnapshot(translog.newSnapshot())) {
                int upTo = sync ? translogOperations : prepareOp;
                for (int i = 0; i < upTo; i++) {
                    Translog.Operation next = snapshot.next();
                    assertThat(next).as("operation " + i + " must be non-null synced: " + sync).isNotNull();
                    assertThat(Integer.parseInt(next.getSource().utf8ToString())).as("payload mismatch, synced: " + sync).isEqualTo(i);
                }
            }
        }
        if (randomBoolean()) { // recover twice
            try (Translog translog = new Translog(config, translogUUID, deletionPolicy,
                () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTerm::get, seqNo -> {})) {
                assertThat(translogGeneration).isNotNull();
                assertThat(translog.currentFileGeneration()).as("lastCommitted must be 3 less than current - we never finished the commit and run recovery twice").isEqualTo(translogGeneration.translogFileGeneration + 3);
                assertThat(translog.syncNeeded()).isFalse();
                try (Translog.Snapshot snapshot = new SortedSnapshot(translog.newSnapshot())) {
                    int upTo = sync ? translogOperations : prepareOp;
                    for (int i = 0; i < upTo; i++) {
                        Translog.Operation next = snapshot.next();
                        assertThat(next).as("operation " + i + " must be non-null synced: " + sync).isNotNull();
                        assertThat(Integer.parseInt(next.getSource().utf8ToString())).as("payload mismatch, synced: " + sync).isEqualTo(i);
                    }
                }
            }
        }
    }


    @Test
    public void testRecoveryUncommittedFileExists() throws IOException {
        List<Translog.Location> locations = new ArrayList<>();
        int translogOperations = randomIntBetween(10, 100);
        final int prepareOp = randomIntBetween(0, translogOperations - 1);
        Translog.TranslogGeneration translogGeneration = null;
        final boolean sync = randomBoolean();
        for (int op = 0; op < translogOperations; op++) {
            locations.add(translog.add(new Translog.Index("" + op, op, primaryTerm.get(),
                Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
            if (op == prepareOp) {
                translogGeneration = translog.getGeneration();
                translog.rollGeneration();
                assertThat(translogGeneration.translogFileGeneration).as("expected this to be the first roll (1 gen is on creation, 2 when opened)").isEqualTo(2L);
                assertThat(translogGeneration.translogUUID).isNotNull();
            }
        }
        if (sync) {
            translog.sync();
        }
        // we intentionally don't close the tlog that is in the prepareCommit stage since we try to recovery the uncommitted
        // translog here as well.
        TranslogConfig config = translog.getConfig();
        Path ckp = config.getTranslogPath().resolve(Translog.CHECKPOINT_FILE_NAME);
        Checkpoint read = Checkpoint.read(ckp);
        Files.copy(ckp, config.getTranslogPath().resolve(Translog.getCommitCheckpointFileName(read.generation)));

        final String translogUUID = translog.getTranslogUUID();
        final TranslogDeletionPolicy deletionPolicy = translog.getDeletionPolicy();
        try (Translog translog = new Translog(config, translogUUID, deletionPolicy,
                () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTerm::get, seqNo -> {})) {
            assertThat(translogGeneration).isNotNull();
            assertThat(translog.currentFileGeneration()).as("lastCommitted must be 2 less than current - we never finished the commit").isEqualTo(translogGeneration.translogFileGeneration + 2);
            assertThat(translog.syncNeeded()).isFalse();
            try (Translog.Snapshot snapshot = new SortedSnapshot(translog.newSnapshot())) {
                int upTo = sync ? translogOperations : prepareOp;
                for (int i = 0; i < upTo; i++) {
                    Translog.Operation next = snapshot.next();
                    assertThat(next).as("operation " + i + " must be non-null synced: " + sync).isNotNull();
                    assertThat(Integer.parseInt(next.getSource().utf8ToString())).as("payload mismatch, synced: " + sync).isEqualTo(i);
                }
            }
        }

        if (randomBoolean()) { // recover twice
            try (Translog translog = new Translog(config, translogUUID, deletionPolicy,
                    () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTerm::get, seqNo -> {})) {
                assertThat(translogGeneration).isNotNull();
                assertThat(translog.currentFileGeneration()).as("lastCommitted must be 3 less than current - we never finished the commit and run recovery twice").isEqualTo(translogGeneration.translogFileGeneration + 3);
                assertThat(translog.syncNeeded()).isFalse();
                try (Translog.Snapshot snapshot = new SortedSnapshot(translog.newSnapshot())) {
                    int upTo = sync ? translogOperations : prepareOp;
                    for (int i = 0; i < upTo; i++) {
                        Translog.Operation next = snapshot.next();
                        assertThat(next).as("operation " + i + " must be non-null synced: " + sync).isNotNull();
                        assertThat(Integer.parseInt(next.getSource().utf8ToString())).as("payload mismatch, synced: " + sync).isEqualTo(i);
                    }
                }
            }
        }
    }

    @Test
    public void testRecoveryUncommittedCorruptedCheckpoint() throws IOException {
        int translogOperations = 100;
        final int prepareOp = 44;
        Translog.TranslogGeneration translogGeneration = null;
        final boolean sync = randomBoolean();
        for (int op = 0; op < translogOperations; op++) {
            translog.add(new Translog.Index("" + op, op, primaryTerm.get(), Integer.toString(op).getBytes(StandardCharsets.UTF_8)));
            if (op == prepareOp) {
                translogGeneration = translog.getGeneration();
                translog.rollGeneration();
                assertThat(translogGeneration.translogFileGeneration).as("expected this to be the first roll (1 gen is on creation, 2 when opened)").isEqualTo(2L);
                assertThat(translogGeneration.translogUUID).isNotNull();
            }
        }
        translog.sync();
        // we intentionally don't close the tlog that is in the prepareCommit stage since we try to recovery the uncommitted
        // translog here as well.
        TranslogConfig config = translog.getConfig();
        Path ckp = config.getTranslogPath().resolve(Translog.CHECKPOINT_FILE_NAME);
        Checkpoint read = Checkpoint.read(ckp);
        Checkpoint corrupted = Checkpoint.emptyTranslogCheckpoint(0, 0,
            SequenceNumbers.NO_OPS_PERFORMED, 0);
        Checkpoint.write(FileChannel::open, config.getTranslogPath().resolve(Translog.getCommitCheckpointFileName(read.generation)),
            corrupted, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        final String translogUUID = translog.getTranslogUUID();
        final TranslogDeletionPolicy deletionPolicy = translog.getDeletionPolicy();
        assertThatThrownBy(() ->
            new Translog(config, translogUUID, deletionPolicy, () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTerm::get, seqNo -> { })
        ).isExactlyInstanceOf(TranslogCorruptedException.class)
            .hasMessageEndingWith(
                "] is corrupted, checkpoint file translog-3.ckp already exists but has corrupted content: expected Checkpoint{offset=2695, " +
                "numOps=55, generation=3, minSeqNo=45, maxSeqNo=99, globalCheckpoint=-1, minTranslogGeneration=1, trimmedAboveSeqNo=-2} " +
                "but got Checkpoint{offset=0, numOps=0, generation=0, minSeqNo=-1, maxSeqNo=-1, globalCheckpoint=-1, " +
                "minTranslogGeneration=0, trimmedAboveSeqNo=-2}");
        Checkpoint.write(FileChannel::open, config.getTranslogPath().resolve(Translog.getCommitCheckpointFileName(read.generation)),
            read, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        try (Translog translog = new Translog(config, translogUUID, deletionPolicy,
                () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTerm::get, seqNo -> {})) {
            assertThat(translogGeneration).isNotNull();
            assertThat(translog.currentFileGeneration()).as("lastCommitted must be 2 less than current - we never finished the commit").isEqualTo(translogGeneration.translogFileGeneration + 2);
            assertThat(translog.syncNeeded()).isFalse();
            try (Translog.Snapshot snapshot = new SortedSnapshot(translog.newSnapshot())) {
                int upTo = sync ? translogOperations : prepareOp;
                for (int i = 0; i < upTo; i++) {
                    Translog.Operation next = snapshot.next();
                    assertThat(next).as("operation " + i + " must be non-null synced: " + sync).isNotNull();
                    assertThat(Integer.parseInt(next.getSource().utf8ToString())).as("payload mismatch, synced: " + sync).isEqualTo(i);
                }
            }
        }
    }

    @Test
    public void testSnapshotFromStreamInput() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        List<Translog.Operation> ops = new ArrayList<>();
        int translogOperations = randomIntBetween(10, 100);
        for (int op = 0; op < translogOperations; op++) {
            Translog.Index test = new Translog.Index("" + op, op, primaryTerm.get(),
                Integer.toString(op).getBytes(Charset.forName("UTF-8")));
            ops.add(test);
        }
        Translog.writeOperations(out, ops);
        final List<Translog.Operation> readOperations = Translog.readOperations(
                out.bytes().streamInput(), "testSnapshotFromStreamInput");
        assertThat(readOperations.size()).isEqualTo(ops.size());
        assertThat(readOperations).isEqualTo(ops);
    }

    @Test
    public void testSnapshotCurrentHasUnexpectedOperationsForTrimmedOperations() throws Exception {
        int extraDocs = randomIntBetween(10, 15);

        // increment primaryTerm to avoid potential negative numbers
        primaryTerm.addAndGet(extraDocs);
        translog.rollGeneration();

        for (int op = 0; op < extraDocs; op++) {
            String ascii = randomAlphaOfLengthBetween(1, 50);
            Translog.Index operation = new Translog.Index("" + op, op, primaryTerm.get() - op,
                ascii.getBytes("UTF-8"));
            translog.add(operation);
        }

        assertThatThrownBy(() -> translog.trimOperations(primaryTerm.get(), 0))
            .isExactlyInstanceOf(AssertionError.class)
            .hasMessage(
                "current should not have any operations with seq#:primaryTerm "
                + "[1:" + (primaryTerm.get() - 1) + "] > 0:" + primaryTerm.get());

        primaryTerm.incrementAndGet();
        translog.rollGeneration();

        // add a single operation to current with seq# > trimmed seq# but higher primary term
        Translog.Index operation = new Translog.Index("" + 1, 1L, primaryTerm.get(),
            randomAlphaOfLengthBetween(1, 50).getBytes("UTF-8"));
        translog.add(operation);

        // it is possible to trim after generation rollover
        translog.trimOperations(primaryTerm.get(), 0);
    }

    @Test
    public void testSnapshotTrimmedOperations() throws Exception {
        final InMemoryTranslog inMemoryTranslog = new InMemoryTranslog();
        final List<Translog.Operation> allOperations = new ArrayList<>();

        for(int attempt = 0, maxAttempts = randomIntBetween(3, 10); attempt < maxAttempts; attempt++) {
            List<Long> ops = LongStream.range(0, allOperations.size() + randomIntBetween(10, 15))
                .boxed().collect(Collectors.toList());
            Randomness.shuffle(ops);

            AtomicReference<String> source = new AtomicReference<>();
            for (final long op : ops) {
                source.set(randomAlphaOfLengthBetween(1, 50));

                // have to use exactly the same source for same seq# if primaryTerm is not changed
                if (primaryTerm.get() == translog.getCurrent().getPrimaryTerm()) {
                    // use the latest source of op with the same seq# - therefore no break
                    allOperations
                        .stream()
                        .filter(allOp -> allOp instanceof Translog.Index && allOp.seqNo() == op)
                        .map(allOp -> ((Translog.Index)allOp).getSource().utf8ToString())
                        .reduce((a, b) -> b)
                        .ifPresent(source::set);
                }

                // use ongoing primaryTerms - or the same as it was
                Translog.Index operation = new Translog.Index("" + op, op, primaryTerm.get(),
                    source.get().getBytes("UTF-8"));
                translog.add(operation);
                inMemoryTranslog.add(operation);
                allOperations.add(operation);
            }

            if (randomBoolean()) {
                primaryTerm.incrementAndGet();
                translog.rollGeneration();
            }

            long maxTrimmedSeqNo = randomInt(allOperations.size());

            translog.trimOperations(primaryTerm.get(), maxTrimmedSeqNo);
            inMemoryTranslog.trimOperations(primaryTerm.get(), maxTrimmedSeqNo);
            translog.sync();

            Collection<Translog.Operation> effectiveOperations = inMemoryTranslog.operations();

            try (Translog.Snapshot snapshot = translog.newSnapshot()) {
                assertThat(snapshot).containsOperationsInAnyOrder(effectiveOperations);
                assertThat(snapshot.totalOperations()).isEqualTo(allOperations.size());
                assertThat(snapshot.skippedOperations()).isEqualTo(allOperations.size() - effectiveOperations.size());
            }
        }
    }

    /**
     * this class mimic behaviour of original {@link Translog}
     */
    static class InMemoryTranslog {
        private final Map<Long, Translog.Operation> operations = new HashMap<>();

        void add(Translog.Operation operation) {
            final Translog.Operation old = operations.put(operation.seqNo(), operation);
            assert old == null || old.primaryTerm() <= operation.primaryTerm();
        }

        void trimOperations(long belowTerm, long aboveSeqNo) {
            for (final Iterator<Map.Entry<Long, Translog.Operation>> it = operations.entrySet().iterator(); it.hasNext(); ) {
                final Map.Entry<Long, Translog.Operation> next = it.next();
                Translog.Operation op = next.getValue();
                boolean drop = op.primaryTerm() < belowTerm && op.seqNo() > aboveSeqNo;
                if (drop) {
                    it.remove();
                }
            }
        }

        Collection<Translog.Operation> operations() {
            return operations.values();
        }
    }

    @Test
    public void testRandomExceptionsOnTrimOperations( ) throws Exception {
        Path tempDir = createTempDir();
        final FailSwitch fail = new FailSwitch();
        fail.failNever();
        TranslogConfig config = getTranslogConfig(tempDir);
        List<FileChannel> fileChannels = new ArrayList<>();
        final Translog failableTLog =
            getFailableTranslog(fail, config, randomBoolean(),
                false, null, createTranslogDeletionPolicy(), fileChannels);

        IOException expectedException = null;
        int translogOperations = 0;
        final int maxAttempts = 10;
        for(int attempt = 0; attempt < maxAttempts; attempt++) {
            int maxTrimmedSeqNo;
            fail.failNever();
            int extraTranslogOperations = randomIntBetween(10, 100);

            List<Integer> ops = IntStream.range(translogOperations, translogOperations + extraTranslogOperations)
                .boxed().collect(Collectors.toList());
            Randomness.shuffle(ops);
            for (int op : ops) {
                String ascii = randomAlphaOfLengthBetween(1, 50);
                Translog.Index operation = new Translog.Index("" + op, op,
                    primaryTerm.get(), ascii.getBytes("UTF-8"));

                failableTLog.add(operation);
            }

            translogOperations += extraTranslogOperations;

            // at least one roll + inc of primary term has to be there - otherwise trim would not take place at all
            // last attempt we have to make roll as well - otherwise could skip trimming as it has been trimmed already
            boolean rollover = attempt == 0 || attempt == maxAttempts - 1 || randomBoolean();
            if (rollover) {
                primaryTerm.incrementAndGet();
                failableTLog.rollGeneration();
            }

            maxTrimmedSeqNo = rollover ? translogOperations - randomIntBetween(4, 8) : translogOperations + 1;

            // if we are so happy to reach the max attempts - fail it always`
            fail.failRate(attempt < maxAttempts - 1 ? 25 : 100);
            try {
                failableTLog.trimOperations(primaryTerm.get(), maxTrimmedSeqNo);
            } catch (IOException e){
                expectedException = e;
                break;
            }
        }

        assertThat(expectedException).isNotNull();
        assertThat(failableTLog.getTragicException()).isEqualTo(expectedException);
        assertThat(fileChannels).isNotEmpty();
        assertThat(fileChannels.stream().filter(f -> f.isOpen()).findFirst())
          .as("all file channels have to be closed")
          .isNotPresent();

        assertThat(failableTLog.isOpen()).isFalse();
        assertThatThrownBy(() -> failableTLog.newSnapshot())
            .isExactlyInstanceOf(AlreadyClosedException.class)
            .hasMessage("translog is already closed");

        fail.failNever();

        // check that despite of IO exception translog is not corrupted
        try(Translog reopenedTranslog = openTranslog(config, failableTLog.getTranslogUUID())) {
            try (Translog.Snapshot snapshot = reopenedTranslog.newSnapshot()) {
                assertThat(snapshot.totalOperations()).isGreaterThan(0);
                Translog.Operation operation;
                for (int i = 0; (operation = snapshot.next()) != null; i++) {
                    assertThat(operation).as("operation " + i + " must be non-null").isNotNull();
                }
            }
        }
    }

    @Test
    public void testLocationHashCodeEquals() throws IOException {
        List<Translog.Location> locations = new ArrayList<>();
        List<Translog.Location> locations2 = new ArrayList<>();
        int translogOperations = randomIntBetween(10, 100);
        try (Translog translog2 = create(createTempDir())) {
            for (int op = 0; op < translogOperations; op++) {
                locations.add(translog.add(new Translog.Index("" + op, op, primaryTerm.get(),
                    Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
                locations2.add(translog2.add(new Translog.Index("" + op, op, primaryTerm.get(),
                    Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
            }
            int iters = randomIntBetween(10, 100);
            for (int i = 0; i < iters; i++) {
                Translog.Location location = RandomPicks.randomFrom(random(), locations);
                for (Translog.Location loc : locations) {
                    if (loc == location) {
                        assertThat(loc.equals(location)).isTrue();
                        assertThat(location.hashCode()).isEqualTo(loc.hashCode());
                    } else {
                        assertThat(loc.equals(location)).isFalse();
                    }
                }
                for (int j = 0; j < translogOperations; j++) {
                    assertThat(locations.get(j).equals(locations2.get(j))).isTrue();
                    assertThat(locations2.get(j).hashCode()).isEqualTo(locations.get(j).hashCode());
                }
            }
        }
    }

    @Test
    public void testOpenForeignTranslog() throws IOException {
        List<Translog.Location> locations = new ArrayList<>();
        int translogOperations = randomIntBetween(1, 10);
        int firstUncommitted = 0;
        for (int op = 0; op < translogOperations; op++) {
            locations.add(translog.add(new Translog.Index("" + op, op, primaryTerm.get(),
                Integer.toString(op).getBytes(Charset.forName("UTF-8")))));
            if (randomBoolean()) {
                translog.rollGeneration();
                translog.getDeletionPolicy().setLocalCheckpointOfSafeCommit(op);
                translog.trimUnreferencedReaders();
                firstUncommitted = op + 1;
            }
        }
        final TranslogConfig config = translog.getConfig();
        final String translogUUID = translog.getTranslogUUID();
        final TranslogDeletionPolicy deletionPolicy = translog.getDeletionPolicy();
        Translog.TranslogGeneration translogGeneration = translog.getGeneration();
        translog.close();

        final String foreignTranslog = randomRealisticUnicodeOfCodepointLengthBetween(1,
            translogGeneration.translogUUID.length());
        try {
            new Translog(config, foreignTranslog, createTranslogDeletionPolicy(), () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTerm::get,
                seqNo -> {});
            fail("translog doesn't belong to this UUID");
        } catch (TranslogCorruptedException ex) {

        }
        this.translog = new Translog(config, translogUUID, deletionPolicy, () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTerm::get,
            seqNo -> {});
        try (Translog.Snapshot snapshot = this.translog.newSnapshot(randomLongBetween(0, firstUncommitted), Long.MAX_VALUE)) {
            for (int i = firstUncommitted; i < translogOperations; i++) {
                Translog.Operation next = snapshot.next();
                assertThat(next).as("" + i).isNotNull();
                assertThat(i).isEqualTo(Integer.parseInt(next.getSource().utf8ToString()));
            }
            assertThat(snapshot.next()).isNull();
        }
    }

    @Test
    public void testFailOnClosedWrite() throws IOException {
        translog.add(new Translog.Index("1", 0, primaryTerm.get(),
            Integer.toString(1).getBytes(Charset.forName("UTF-8"))));
        translog.close();
        try {
            translog.add(new Translog.Index("1", 0, primaryTerm.get(),
                Integer.toString(1).getBytes(Charset.forName("UTF-8"))));
            fail("closed");
        } catch (AlreadyClosedException ex) {
            // all is well
        }
    }

    @Test
    public void testCloseConcurrently() throws Throwable {
        final int opsPerThread = randomIntBetween(10, 200);
        int threadCount = 2 + randomInt(5);

        logger.info("testing with [{}] threads, each doing [{}] ops", threadCount, opsPerThread);
        final BlockingQueue<LocationOperation> writtenOperations = new ArrayBlockingQueue<>(threadCount * opsPerThread);

        Thread[] threads = new Thread[threadCount];
        final Exception[] threadExceptions = new Exception[threadCount];
        final CountDownLatch downLatch = new CountDownLatch(1);
        final AtomicLong seqNoGenerator = new AtomicLong();
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            threads[i] = new TranslogThread(translog, downLatch, opsPerThread, threadId,
                writtenOperations, seqNoGenerator, threadExceptions);
            threads[i].setDaemon(true);
            threads[i].start();
        }

        downLatch.countDown();
        translog.close();

        for (int i = 0; i < threadCount; i++) {
            if (threadExceptions[i] != null) {
                if ((threadExceptions[i] instanceof AlreadyClosedException) == false) {
                    throw threadExceptions[i];
                }
            }
            threads[i].join(60 * 1000);
        }
    }

    private class TranslogThread extends Thread {
        private final CountDownLatch downLatch;
        private final int opsPerThread;
        private final int threadId;
        private final Collection<LocationOperation> writtenOperations;
        private final Exception[] threadExceptions;
        private final Translog translog;
        private final AtomicLong seqNoGenerator;

        TranslogThread(Translog translog, CountDownLatch downLatch, int opsPerThread, int threadId,
                       Collection<LocationOperation> writtenOperations, AtomicLong seqNoGenerator, Exception[] threadExceptions) {
            this.translog = translog;
            this.downLatch = downLatch;
            this.opsPerThread = opsPerThread;
            this.threadId = threadId;
            this.writtenOperations = writtenOperations;
            this.seqNoGenerator = seqNoGenerator;
            this.threadExceptions = threadExceptions;
        }

        @Override
        public void run() {
            try {
                downLatch.await();
                for (int opCount = 0; opCount < opsPerThread; opCount++) {
                    Translog.Operation op;
                    final Translog.Operation.Type type = randomFrom(Translog.Operation.Type.values());
                    switch (type) {
                        case CREATE:
                        case INDEX:
                            op = new Translog.Index(threadId + "_" + opCount, seqNoGenerator.getAndIncrement(),
                                primaryTerm.get(), RandomizedTest.randomUnicodeOfLengthBetween(1, 20 * 1024).getBytes("UTF-8"));
                            break;
                        case DELETE:
                            op = new Translog.Delete(
                                threadId + "_" + opCount,
                                new Term("_uid", threadId + "_" + opCount),
                                seqNoGenerator.getAndIncrement(),
                                primaryTerm.get(),
                                1 + randomInt(100000));
                            break;
                        case NO_OP:
                            op = new Translog.NoOp(seqNoGenerator.getAndIncrement(), primaryTerm.get(), randomAlphaOfLength(16));
                            break;
                        default:
                            throw new AssertionError("unsupported operation type [" + type + "]");
                    }

                    Translog.Location loc = add(op);
                    writtenOperations.add(new LocationOperation(op, loc));
                    if (rarely()) { // lets verify we can concurrently read this
                        assertThat(translog.readOperation(loc)).isEqualTo(op);
                    }
                    afterAdd();
                }
            } catch (Exception t) {
                threadExceptions[threadId] = t;
            }
        }

        protected Translog.Location add(Translog.Operation op) throws IOException {
            return translog.add(op);
        }

        protected void afterAdd() throws IOException {
        }
    }

    @Test
    public void testFailFlush() throws IOException {
        Path tempDir = createTempDir();
        final FailSwitch fail = new FailSwitch();
        TranslogConfig config = getTranslogConfig(tempDir);
        Translog translog = getFailableTranslog(fail, config);

        List<Translog.Location> locations = new ArrayList<>();
        int opsSynced = 0;
        boolean failed = false;
        while (failed == false) {
            try {
                locations.add(translog.add(
                    new Translog.Index("" + opsSynced, opsSynced, primaryTerm.get(),
                        Integer.toString(opsSynced).getBytes(Charset.forName("UTF-8")))));
                translog.sync();
                opsSynced++;
            } catch (MockDirectoryWrapper.FakeIOException ex) {
                failed = true;
                assertThat(translog.isOpen()).isFalse();
            } catch (IOException ex) {
                failed = true;
                assertThat(translog.isOpen()).isFalse();
                assertThat(ex.getMessage()).isEqualTo("__FAKE__ no space left on device");
            }
            if (randomBoolean()) {
                fail.failAlways();
            } else {
                fail.failNever();
            }
        }
        fail.failNever();
        if (randomBoolean()) {
            try {
                locations.add(translog.add(
                    new Translog.Index("" + opsSynced, opsSynced, primaryTerm.get(),
                        Integer.toString(opsSynced).getBytes(Charset.forName("UTF-8")))));
                fail("we are already closed");
            } catch (AlreadyClosedException ex) {
                assertThat(ex.getCause()).isNotNull();
                if (ex.getCause() instanceof MockDirectoryWrapper.FakeIOException) {
                    assertThat(ex.getCause().getMessage()).isNull();
                } else {
                    assertThat("__FAKE__ no space left on device").isEqualTo(ex.getCause().getMessage());
                }
            }

        }
        Translog.TranslogGeneration translogGeneration = translog.getGeneration();
        try {
            translog.newSnapshot();
            fail("already closed");
        } catch (AlreadyClosedException ex) {
            // all is well
            assertThat(ex.getCause())
                .isNotNull()
                .isSameAs(translog.getTragicException());
        }

        try {
            translog.rollGeneration();
            fail("already closed");
        } catch (AlreadyClosedException ex) {
            assertThat(ex.getCause())
                .isNotNull()
                .isSameAs(translog.getTragicException());
        }

        assertThat(translog.isOpen()).isFalse();
        translog.close(); // we are closed
        final String translogUUID = translog.getTranslogUUID();
        final TranslogDeletionPolicy deletionPolicy = translog.getDeletionPolicy();
        try (Translog tlog = new Translog(config, translogUUID, deletionPolicy,
                () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTerm::get, seqNo -> {})) {
            assertThat(tlog.currentFileGeneration()).as("lastCommitted must be 1 less than current").isEqualTo(translogGeneration.translogFileGeneration + 1);
            assertThat(tlog.syncNeeded()).isFalse();

            try (Translog.Snapshot snapshot = tlog.newSnapshot()) {
                assertThat(snapshot.totalOperations()).isEqualTo(opsSynced);
                for (int i = 0; i < opsSynced; i++) {
                    assertThat(locations.get(i).generation).as("expected operation" + i + " to be in the previous translog but wasn't").isEqualTo(tlog.currentFileGeneration() - 1);
                    Translog.Operation next = snapshot.next();
                    assertThat(next).as("operation " + i + " must be non-null").isNotNull();
                    assertThat(Integer.parseInt(next.getSource().utf8ToString())).isEqualTo(i);
                }
            }
        }
    }

    @Test
    public void testTranslogOpsCountIsCorrect() throws IOException {
        List<Translog.Location> locations = new ArrayList<>();
        int numOps = randomIntBetween(100, 200);
        LineFileDocs lineFileDocs = new LineFileDocs(random()); // writes pretty big docs so we cross buffer borders regularly
        for (int opsAdded = 0; opsAdded < numOps; opsAdded++) {
            locations.add(translog.add(
                new Translog.Index("" + opsAdded, opsAdded, primaryTerm.get(),
                    lineFileDocs.nextDoc().toString().getBytes(Charset.forName("UTF-8")))));
            try (Translog.Snapshot snapshot = this.translog.newSnapshot()) {
                assertThat(snapshot.totalOperations()).isEqualTo(opsAdded + 1);
                for (int i = 0; i < opsAdded; i++) {
                    assertThat(locations.get(i).generation).as("expected operation" + i + " to be in the current translog but wasn't").isEqualTo(translog.currentFileGeneration());
                    Translog.Operation next = snapshot.next();
                    assertThat(next).as("operation " + i + " must be non-null").isNotNull();
                }
            }
        }
    }

    @Test
    public void testTragicEventCanBeAnyException() throws IOException {
        Path tempDir = createTempDir();
        final FailSwitch fail = new FailSwitch();
        TranslogConfig config = getTranslogConfig(tempDir);
        Translog translog = getFailableTranslog(fail, config, false, true, null,
            createTranslogDeletionPolicy());
        LineFileDocs lineFileDocs = new LineFileDocs(random()); // writes pretty big docs so we cross buffer boarders regularly
        translog.add(new Translog.Index("1", 0, primaryTerm.get(),
            lineFileDocs.nextDoc().toString().getBytes(Charset.forName("UTF-8"))));
        fail.failAlways();
        try {
            Translog.Location location = translog.add(
                new Translog.Index("2", 1, primaryTerm.get(),
                    lineFileDocs.nextDoc().toString().getBytes(Charset.forName("UTF-8"))));
            if (randomBoolean()) {
                translog.ensureSynced(location);
            } else {
                translog.sync();
            }
            //TODO once we have a mock FS that can simulate we can also fail on plain sync
            fail("WTF");
        } catch (UnknownException ex) {
            // w00t
        } catch (TranslogException ex) {
            assertThat(ex.getCause() instanceof UnknownException).isTrue();
        }
        assertThat(translog.isOpen()).isFalse();
        assertThat(translog.getTragicException() instanceof UnknownException).isTrue();
    }

    @Test
    public void testFatalIOExceptionsWhileWritingConcurrently() throws IOException, InterruptedException {
        Path tempDir = createTempDir();
        final FailSwitch fail = new FailSwitch();

        TranslogConfig config = getTranslogConfig(tempDir);
        Translog translog = getFailableTranslog(fail, config);
        final String translogUUID = translog.getTranslogUUID();

        final int threadCount = randomIntBetween(1, 5);
        Thread[] threads = new Thread[threadCount];
        final Exception[] threadExceptions = new Exception[threadCount];
        final CountDownLatch downLatch = new CountDownLatch(1);
        final CountDownLatch added = new CountDownLatch(randomIntBetween(10, 100));
        final AtomicLong seqNoGenerator = new AtomicLong();
        List<LocationOperation> writtenOperations = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            threads[i] = new TranslogThread(translog, downLatch, 200, threadId,
                    writtenOperations, seqNoGenerator, threadExceptions) {
                @Override
                protected Translog.Location add(Translog.Operation op) throws IOException {
                    Translog.Location add = super.add(op);
                    added.countDown();
                    return add;
                }

                @Override
                protected void afterAdd() throws IOException {
                    if (randomBoolean()) {
                        translog.sync();
                    }
                }
            };
            threads[i].setDaemon(true);
            threads[i].start();
        }
        downLatch.countDown();
        added.await();
        try (Closeable ignored = translog.acquireRetentionLock()) {
            // this holds a reference to the current tlog channel such that it's not closed
            // if we hit a tragic event. this is important to ensure that asserts inside the Translog#add doesn't trip
            // otherwise our assertions here are off by one sometimes.
            fail.failAlways();
            for (int i = 0; i < threadCount; i++) {
                threads[i].join();
            }
            boolean atLeastOneFailed = false;
            for (Throwable ex : threadExceptions) {
                if (ex != null) {
                    assertThat(ex instanceof IOException || ex instanceof AlreadyClosedException).as(ex.toString()).isTrue();
                    atLeastOneFailed = true;
                }
            }
            if (atLeastOneFailed == false) {
                try {
                    boolean syncNeeded = translog.syncNeeded();
                    translog.close();
                    assertThat(syncNeeded).as("should have failed if sync was needed").isFalse();
                } catch (IOException ex) {
                    // boom now we failed
                }
            }
            Collections.sort(writtenOperations, (a, b) -> a.location.compareTo(b.location));
            assertThat(translog.isOpen()).isFalse();
            final Checkpoint checkpoint = Checkpoint.read(config.getTranslogPath().resolve(Translog.CHECKPOINT_FILE_NAME));
            // drop all that haven't been synced
            writtenOperations.removeIf(next -> checkpoint.offset < (next.location.translogLocation + next.location.size));
            try (Translog tlog =
                     new Translog(config, translogUUID, createTranslogDeletionPolicy(),
                         () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTerm::get, seqNo -> {});
                 Translog.Snapshot snapshot = tlog.newSnapshot()) {
                if (writtenOperations.size() != snapshot.totalOperations()) {
                    for (int i = 0; i < threadCount; i++) {
                        if (threadExceptions[i] != null) {
                            logger.info("Translog exception", threadExceptions[i]);
                        }
                    }
                }
                assertThat(snapshot.totalOperations()).isEqualTo(writtenOperations.size());
                for (int i = 0; i < writtenOperations.size(); i++) {
                    assertThat(writtenOperations.get(i).location.generation).as("expected operation" + i + " to be in the previous translog but wasn't").isEqualTo(tlog.currentFileGeneration() - 1);
                    Translog.Operation next = snapshot.next();
                    assertThat(next).as("operation " + i + " must be non-null").isNotNull();
                    assertThat(writtenOperations.get(i).operation).isEqualTo(next);
                }
            }
        }
    }

    /**
     * Tests the situation where the node crashes after a translog gen was committed to lucene, but before the translog had the chance
     * to clean up its files.
     */
    @Test
    public void testRecoveryFromAFutureGenerationCleansUp() throws IOException {
        int translogOperations = randomIntBetween(10, 100);
        int op = 0;
        for (; op < translogOperations / 2; op++) {
            translog.add(new Translog.Index(Integer.toString(op), op, primaryTerm.get(),
                Integer.toString(op).getBytes(Charset.forName("UTF-8"))));
            if (rarely()) {
                translog.rollGeneration();
            }
        }
        translog.rollGeneration();
        long localCheckpoint = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, op);
        for (op = translogOperations / 2; op < translogOperations; op++) {
            translog.add(new Translog.Index(Integer.toString(op), op, primaryTerm.get(),
                Integer.toString(op).getBytes(Charset.forName("UTF-8"))));
            if (rarely()) {
                translog.rollGeneration();
            }
        }
        long minRetainedGen = translog.getMinGenerationForSeqNo(localCheckpoint + 1).translogFileGeneration;
        // engine blows up, after committing the above generation
        translog.close();
        TranslogConfig config = translog.getConfig();
        final TranslogDeletionPolicy deletionPolicy = new TranslogDeletionPolicy(-1, -1, 0);
        deletionPolicy.setLocalCheckpointOfSafeCommit(localCheckpoint);
        translog = new Translog(config, translog.getTranslogUUID(), deletionPolicy,
            () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTerm::get, seqNo -> {});
        assertThat(translog.getMinFileGeneration()).isEqualTo(1L);
        // no trimming done yet, just recovered
        for (long gen = 1; gen < translog.currentFileGeneration(); gen++) {
            assertFileIsPresent(translog, gen);
        }
        translog.trimUnreferencedReaders();
        for (long gen = 1; gen < minRetainedGen; gen++) {
            assertFileDeleted(translog, gen);
        }
    }

    /**
     * Tests the situation where the node crashes after a translog gen was committed to lucene, but before the translog had the chance
     * to clean up its files.
     */
    @Test
    public void testRecoveryFromFailureOnTrimming() throws IOException {
        Path tempDir = createTempDir();
        final FailSwitch fail = new FailSwitch();
        fail.failNever();
        final TranslogConfig config = getTranslogConfig(tempDir);
        final long localCheckpoint;
        final String translogUUID;
        long minGenForRecovery = 1L;
        try (Translog translog = getFailableTranslog(fail, config)) {
            final TranslogDeletionPolicy deletionPolicy = translog.getDeletionPolicy();
            // disable retention so we trim things
            deletionPolicy.setRetentionSizeInBytes(-1);
            deletionPolicy.setRetentionAgeInMillis(-1);
            translogUUID = translog.getTranslogUUID();
            int translogOperations = randomIntBetween(10, 100);
            int op = 0;
            for (; op < translogOperations / 2; op++) {
                translog.add(new Translog.Index(Integer.toString(op), op, primaryTerm.get(),
                    Integer.toString(op).getBytes(Charset.forName("UTF-8"))));
                if (rarely()) {
                    translog.rollGeneration();
                }
            }
            translog.rollGeneration();
            localCheckpoint = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, op);
            for (op = translogOperations / 2; op < translogOperations; op++) {
                translog.add(new Translog.Index(Integer.toString(op), op, primaryTerm.get(),
                    Integer.toString(op).getBytes(Charset.forName("UTF-8"))));
                if (rarely()) {
                    translog.rollGeneration();
                }
            }
            deletionPolicy.setLocalCheckpointOfSafeCommit(localCheckpoint);
            minGenForRecovery = translog.getMinGenerationForSeqNo(localCheckpoint + 1).translogFileGeneration;
            fail.failRandomly();
            try {
                translog.trimUnreferencedReaders();
            } catch (Exception e) {
                // expected...
            }
        }
        final TranslogDeletionPolicy deletionPolicy = new TranslogDeletionPolicy(-1, -1, 0);
        deletionPolicy.setLocalCheckpointOfSafeCommit(localCheckpoint);
        try (Translog translog = new Translog(config, translogUUID, deletionPolicy,
            () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTerm::get, seqNo -> {})) {
            // we don't know when things broke exactly
            assertThat(translog.getMinFileGeneration()).isGreaterThanOrEqualTo(1L);
            assertThat(translog.getMinFileGeneration()).isLessThanOrEqualTo(minGenForRecovery);
            assertFilePresences(translog);
            minGenForRecovery = translog.getMinGenerationForSeqNo(localCheckpoint + 1).translogFileGeneration;
            translog.trimUnreferencedReaders();
            assertThat(translog.getMinFileGeneration()).isEqualTo(minGenForRecovery);
            assertFilePresences(translog);
        }
    }

    private Translog getFailableTranslog(FailSwitch fail, final TranslogConfig config) throws IOException {
        return getFailableTranslog(fail, config, randomBoolean(), false, null, createTranslogDeletionPolicy());
    }

    private static class FailSwitch {
        private volatile int failRate;
        private volatile boolean onceFailedFailAlways = false;

        public boolean fail() {
            final int rnd = randomIntBetween(1, 100);
            boolean fail = rnd <= failRate;
            if (fail && onceFailedFailAlways) {
                failAlways();
            }
            return fail;
        }

        public void failNever() {
            failRate = 0;
        }

        public void failAlways() {
            failRate = 100;
        }

        public void failRandomly() {
            failRate = randomIntBetween(1, 100);
        }

        public void failRate(int rate) {
            failRate = rate;
        }

        public void onceFailedFailAlways() {
            onceFailedFailAlways = true;
        }
    }

    private Translog getFailableTranslog(FailSwitch fail,
                                         TranslogConfig config,
                                         boolean partialWrites,
                                         boolean throwUnknownException,
                                         String translogUUID,
                                         TranslogDeletionPolicy deletionPolicy) throws IOException {
        return getFailableTranslog(fail, config, partialWrites, throwUnknownException, translogUUID, deletionPolicy, null);
    }

    private Translog getFailableTranslog(FailSwitch fail,
                                         TranslogConfig config,
                                         boolean partialWrites,
                                         boolean throwUnknownException,
                                         String translogUUID,
                                         TranslogDeletionPolicy deletionPolicy,
                                         List<FileChannel> fileChannels) throws IOException {
        final ChannelFactory channelFactory = (file, openOption) -> {
            FileChannel channel = FileChannel.open(file, openOption);
            if (fileChannels != null) {
                fileChannels.add(channel);
            }
            boolean success = false;
            try {
                // don't do partial writes for checkpoints we rely on the fact that the bytes are written as an atomic operation
                final boolean isCkpFile = file.getFileName().toString().endsWith(".ckp");
                ThrowingFileChannel throwingFileChannel =
                    new ThrowingFileChannel(fail, isCkpFile ? false : partialWrites, throwUnknownException, channel);
                success = true;
                return throwingFileChannel;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(channel);
                }
            }
        };
        if (translogUUID == null) {
            translogUUID = Translog.createEmptyTranslog(
                config.getTranslogPath(), SequenceNumbers.NO_OPS_PERFORMED, shardId, channelFactory, primaryTerm.get());
        }
        return new Translog(config, translogUUID, deletionPolicy, () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTerm::get,
            seqNo -> {}) {
            @Override
            ChannelFactory getChannelFactory() {
                return channelFactory;
            }

            @Override
            void deleteReaderFiles(TranslogReader reader) {
                if (fail.fail()) {
                    // simulate going OOM and dying just at the wrong moment.
                    throw new RuntimeException("simulated");
                } else {
                    super.deleteReaderFiles(reader);
                }
            }
        };
    }

    public static class ThrowingFileChannel extends FilterFileChannel {
        private final FailSwitch fail;
        private final boolean partialWrite;
        private final boolean throwUnknownException;

        public ThrowingFileChannel(FailSwitch fail, boolean partialWrite,
                                        boolean throwUnknownException, FileChannel delegate) throws MockDirectoryWrapper.FakeIOException {
            super(delegate);
            this.fail = fail;
            this.partialWrite = partialWrite;
            this.throwUnknownException = throwUnknownException;
            if (fail.fail()) {
                throw new MockDirectoryWrapper.FakeIOException();
            }
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            if (fail.fail()) {
                throw new MockDirectoryWrapper.FakeIOException();
            }
            return super.read(dst);
        }

        @Override
        public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
            if (fail.fail()) {
                throw new MockDirectoryWrapper.FakeIOException();
            }
            return super.read(dsts, offset, length);
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int write(ByteBuffer src, long position) throws IOException {
            if (fail.fail()) {
                if (partialWrite) {
                    if (src.hasRemaining()) {
                        final int pos = src.position();
                        final int limit = src.limit();
                        src.limit(randomIntBetween(pos, limit));
                        super.write(src, position);
                        src.limit(limit);
                        src.position(pos);
                        throw new IOException("__FAKE__ no space left on device");
                    }
                }
                if (throwUnknownException) {
                    throw new UnknownException();
                } else {
                    throw new MockDirectoryWrapper.FakeIOException();
                }
            }
            return super.write(src, position);
        }


        @Override
        public int write(ByteBuffer src) throws IOException {
            if (fail.fail()) {
                if (partialWrite) {
                    if (src.hasRemaining()) {
                        final int pos = src.position();
                        final int limit = src.limit();
                        src.limit(randomIntBetween(pos, limit));
                        super.write(src);
                        src.limit(limit);
                        src.position(pos);
                        throw new IOException("__FAKE__ no space left on device");
                    }
                }
                if (throwUnknownException) {
                    throw new UnknownException();
                } else {
                    throw new MockDirectoryWrapper.FakeIOException();
                }
            }
            return super.write(src);
        }

        @Override
        public void force(boolean metaData) throws IOException {
            if (fail.fail()) {
                if (throwUnknownException) {
                    throw new UnknownException();
                } else {
                    throw new MockDirectoryWrapper.FakeIOException();
                }
            }
            super.force(metaData);
        }

        @Override
        public long position() throws IOException {
            if (fail.fail()) {
                if (throwUnknownException) {
                    throw new UnknownException();
                } else {
                    throw new MockDirectoryWrapper.FakeIOException();
                }
            }
            return super.position();
        }
    }

    private static final class UnknownException extends RuntimeException {

    }

    // see https://github.com/elastic/elasticsearch/issues/15754
    @Test
    public void testFailWhileCreateWriteWithRecoveredTLogs() throws IOException {
        Path tempDir = createTempDir();
        TranslogConfig config = getTranslogConfig(tempDir);
        Translog translog = createTranslog(config);
        translog.add(new Translog.Index("boom", 0, primaryTerm.get(), "boom".getBytes(Charset.forName("UTF-8"))));
        translog.close();
        try (var _ = new Translog(
                config,
                translog.getTranslogUUID(),
                createTranslogDeletionPolicy(),
                () -> SequenceNumbers.NO_OPS_PERFORMED,
                primaryTerm::get,
                _ -> {}
            ) {
                @Override
                protected TranslogWriter createWriter(long fileGeneration,
                                                      long initialMinTranslogGen,
                                                      long initialGlobalCheckpoint,
                                                      LongConsumer persistedSequenceNumberConsumer) {
                    throw new TranslogException(shardId, "Failed to create translog", new MockDirectoryWrapper.FakeIOException());
                }
            }) {
            // if we have a LeakFS here we fail if not all resources are closed
            fail("should have been failed");
        } catch (TranslogException ex) {
            // all is well
        }
    }

    @Test
    public void testRecoverWithUnbackedNextGen() throws IOException {
        translog.add(new Translog.Index("" + 0, 0, primaryTerm.get(),
            Integer.toString(1).getBytes(Charset.forName("UTF-8"))));
        translog.close();
        TranslogConfig config = translog.getConfig();

        Path ckp = config.getTranslogPath().resolve(Translog.CHECKPOINT_FILE_NAME);
        Checkpoint read = Checkpoint.read(ckp);
        Files.copy(ckp, config.getTranslogPath().resolve(Translog.getCommitCheckpointFileName(read.generation)));
        Files.createFile(config.getTranslogPath().resolve("translog-" + (read.generation + 1) + ".tlog"));
        try (Translog tlog = openTranslog(config, translog.getTranslogUUID());
             Translog.Snapshot snapshot = tlog.newSnapshot()) {
            assertThat(tlog.syncNeeded()).isFalse();

            Translog.Operation op = snapshot.next();
            assertThat(op).as("operation 1 must be non-null").isNotNull();
            assertThat(Integer.parseInt(op.getSource().utf8ToString())).as("payload mismatch for operation 1").isEqualTo(1);

            tlog.add(new Translog.Index("" + 1, 1, primaryTerm.get(),
                Integer.toString(2).getBytes(Charset.forName("UTF-8"))));
        }

        try (Translog tlog = openTranslog(config, translog.getTranslogUUID());
             Translog.Snapshot snapshot = tlog.newSnapshot()) {
            assertThat(tlog.syncNeeded()).isFalse();

            Translog.Operation secondOp = snapshot.next();
            assertThat(secondOp).as("operation 2 must be non-null").isNotNull();
            assertThat(2).as("payload mismatch for operation 2").isEqualTo(Integer.parseInt(secondOp.getSource().utf8ToString()));

            Translog.Operation firstOp = snapshot.next();
            assertThat(firstOp).as("operation 1 must be non-null").isNotNull();
            assertThat(1).as("payload mismatch for operation 1").isEqualTo(Integer.parseInt(firstOp.getSource().utf8ToString()));
        }
    }

    @Test
    public void testRecoverWithUnbackedNextGenInIllegalState() throws IOException {
        translog.add(new Translog.Index("" + 0, 0, primaryTerm.get(),
            Integer.toString(0).getBytes(Charset.forName("UTF-8"))));
        translog.close();
        TranslogConfig config = translog.getConfig();
        Path ckp = config.getTranslogPath().resolve(Translog.CHECKPOINT_FILE_NAME);
        Checkpoint read = Checkpoint.read(ckp);
        // don't copy the new file
        Files.createFile(config.getTranslogPath().resolve("translog-" + (read.generation + 1) + ".tlog"));

        assertThatThrownBy(() -> new Translog(
            config,
            translog.getTranslogUUID(),
            translog.getDeletionPolicy(),
            () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTerm::get, seqNo -> {})
        ).isExactlyInstanceOf(TranslogException.class)
            .hasMessage("failed to create new translog file")
            .hasCauseExactlyInstanceOf(FileAlreadyExistsException.class);
    }

    @Test
    public void testRecoverWithUnbackedNextGenAndFutureFile() throws IOException {
        translog.add(new Translog.Index("" + 0, 0, primaryTerm.get(),
            Integer.toString(0).getBytes(Charset.forName("UTF-8"))));
        translog.close();
        TranslogConfig config = translog.getConfig();
        final String translogUUID = translog.getTranslogUUID();
        final TranslogDeletionPolicy deletionPolicy = translog.getDeletionPolicy();

        Path ckp = config.getTranslogPath().resolve(Translog.CHECKPOINT_FILE_NAME);
        Checkpoint read = Checkpoint.read(ckp);
        Files.copy(ckp, config.getTranslogPath().resolve(Translog.getCommitCheckpointFileName(read.generation)));
        Files.createFile(config.getTranslogPath().resolve("translog-" + (read.generation + 1) + ".tlog"));
        // we add N+1 and N+2 to ensure we only delete the N+1 file and never jump ahead and wipe without the right condition
        Files.createFile(config.getTranslogPath().resolve("translog-" + (read.generation + 2) + ".tlog"));
        try (Translog tlog = new Translog(config, translogUUID, deletionPolicy,
                () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTerm::get, seqNo -> {})) {
            assertThat(tlog.syncNeeded()).isFalse();
            try (Translog.Snapshot snapshot = tlog.newSnapshot()) {
                for (int i = 0; i < 1; i++) {
                    Translog.Operation next = snapshot.next();
                    assertThat(next).as("operation " + i + " must be non-null").isNotNull();
                    assertThat(Integer.parseInt(next.getSource().utf8ToString())).as("payload missmatch").isEqualTo(i);
                }
            }
            tlog.add(new Translog.Index("" + 1, 1, primaryTerm.get(),
                Integer.toString(1).getBytes(Charset.forName("UTF-8"))));
        }

        assertThatThrownBy(() -> new Translog(
            config,
            translogUUID,
            deletionPolicy,
            () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTerm::get, seqNo -> {})
        ).isExactlyInstanceOf(TranslogException.class)
            .hasMessage("failed to create new translog file")
            .hasCauseExactlyInstanceOf(FileAlreadyExistsException.class);
    }

    /**
     * This test adds operations to the translog which might randomly throw an IOException. The only thing this test verifies is
     * that we can, after we hit an exception, open and recover the translog successfully and retrieve all successfully synced operations
     * from the transaction log.
     */
    @Test
    public void testWithRandomException() throws IOException {
        final int runs = randomIntBetween(5, 10);
        for (int run = 0; run < runs; run++) {
            Path tempDir = createTempDir();
            final FailSwitch fail = new FailSwitch();
            fail.failRandomly();
            TranslogConfig config = getTranslogConfig(tempDir);
            final int numOps = randomIntBetween(100, 200);
            long localCheckpointOfSafeCommit = SequenceNumbers.NO_OPS_PERFORMED;
            List<String> syncedDocs = new ArrayList<>();
            List<String> unsynced = new ArrayList<>();
            if (randomBoolean()) {
                fail.onceFailedFailAlways();
            }
            String generationUUID = null;
            try {
                boolean committing = false;
                final Translog failableTLog = getFailableTranslog(
                    fail,
                    config,
                    randomBoolean(),
                    false,
                    generationUUID,
                    createTranslogDeletionPolicy()
                );
                try {
                    LineFileDocs lineFileDocs = new LineFileDocs(random()); //writes pretty big docs so we cross buffer boarders regularly
                    for (int opsAdded = 0; opsAdded < numOps; opsAdded++) {
                        String doc = lineFileDocs.nextDoc().toString();
                        failableTLog.add(new Translog.Index( "" + opsAdded, opsAdded, primaryTerm.get(),
                            doc.getBytes(Charset.forName("UTF-8"))));
                        unsynced.add(doc);
                        if (randomBoolean()) {
                            failableTLog.sync();
                            syncedDocs.addAll(unsynced);
                            unsynced.clear();
                        }
                        if (randomFloat() < 0.1) {
                            // we have to sync here first otherwise we don't know if the sync succeeded if the commit fails
                            failableTLog.sync();
                            syncedDocs.addAll(unsynced);
                            unsynced.clear();
                            failableTLog.rollGeneration();
                            committing = true;
                            failableTLog.getDeletionPolicy().setLocalCheckpointOfSafeCommit(opsAdded);
                            syncedDocs.clear();
                            failableTLog.trimUnreferencedReaders();
                            committing = false;
                        }
                    }
                    // we survived all the randomness!!!
                    // lets close the translog and if it succeeds we are all synced again. If we don't do this we will close
                    // it in the finally block but miss to copy over unsynced docs to syncedDocs and fail the assertion down the road...
                    failableTLog.close();
                    syncedDocs.addAll(unsynced);
                    unsynced.clear();
                } catch (TranslogException | MockDirectoryWrapper.FakeIOException ex) {
                    assertThat(ex).isEqualTo(failableTLog.getTragicException());
                } catch (IOException ex) {
                    assertThat("__FAKE__ no space left on device").isEqualTo(ex.getMessage());
                    assertThat(ex).isEqualTo(failableTLog.getTragicException());
                } catch (RuntimeException ex) {
                    assertThat("simulated").isEqualTo(ex.getMessage());
                    assertThat(failableTLog.getTragicException())
                        .as("Don't consider failures while trimming unreferenced readers as tragedy")
                        .isNull();
                } finally {
                    Checkpoint checkpoint = Translog.readCheckpoint(config.getTranslogPath());
                    if (checkpoint.numOps == unsynced.size() + syncedDocs.size()) {
                        syncedDocs.addAll(unsynced); // failed in fsync but got fully written
                        unsynced.clear();
                    }
                    if (committing && checkpoint.minTranslogGeneration == checkpoint.generation) {
                        // we were committing and blew up in one of the syncs, but they made it through
                        syncedDocs.clear();
                        assertThat(unsynced).isEmpty();
                    }
                    generationUUID = failableTLog.getTranslogUUID();
                    localCheckpointOfSafeCommit = failableTLog.getDeletionPolicy().getLocalCheckpointOfSafeCommit();
                    IOUtils.closeWhileHandlingException(failableTLog);
                }
            } catch (TranslogException | MockDirectoryWrapper.FakeIOException ex) {
                // failed - that's ok, we didn't even create it
            } catch (IOException ex) {
                assertThat("__FAKE__ no space left on device").isEqualTo(ex.getMessage());
            }
            // now randomly open this failing tlog again just to make sure we can also recover from failing during recovery
            if (randomBoolean()) {
                try {
                    TranslogDeletionPolicy deletionPolicy = createTranslogDeletionPolicy();
                    deletionPolicy.setLocalCheckpointOfSafeCommit(localCheckpointOfSafeCommit);
                    IOUtils.close(getFailableTranslog(fail, config, randomBoolean(), false, generationUUID, deletionPolicy));
                } catch (TranslogException | MockDirectoryWrapper.FakeIOException ex) {
                    // failed - that's ok, we didn't even create it
                } catch (IOException ex) {
                    assertThat("__FAKE__ no space left on device").isEqualTo(ex.getMessage());
                }
            }

            fail.failNever(); // we don't wanna fail here but we might since we write a new checkpoint and create a new tlog file
            TranslogDeletionPolicy deletionPolicy = createTranslogDeletionPolicy();
            deletionPolicy.setLocalCheckpointOfSafeCommit(localCheckpointOfSafeCommit);
            if (generationUUID == null) {
                // we never managed to successfully create a translog, make it
                generationUUID = Translog.createEmptyTranslog(config.getTranslogPath(),
                    SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
            }
            try (Translog translog = new Translog(config, generationUUID, deletionPolicy,
                () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTerm::get, seqNo -> {});
                 Translog.Snapshot snapshot = translog.newSnapshot(localCheckpointOfSafeCommit + 1, Long.MAX_VALUE)) {
                assertThat(snapshot.totalOperations()).isEqualTo(syncedDocs.size());
                for (int i = 0; i < syncedDocs.size(); i++) {
                    Translog.Operation next = snapshot.next();
                    assertThat(next.getSource().utf8ToString()).isEqualTo(syncedDocs.get(i));
                    assertThat(next).as("operation " + i + " must be non-null").isNotNull();
                }
            }
        }
    }

    private Checkpoint randomCheckpoint() {
        final long a = randomNonNegativeLong();
        final long b = randomNonNegativeLong();
        final long minSeqNo;
        final long maxSeqNo;
        if (a <= b) {
            minSeqNo = a;
            maxSeqNo = b;
        } else {
            minSeqNo = b;
            maxSeqNo = a;
        }
        final long generation = randomNonNegativeLong();
        return new Checkpoint(randomLong(), randomInt(), generation, minSeqNo, maxSeqNo, randomNonNegativeLong(),
            randomLongBetween(1, generation), maxSeqNo);
    }

    @Test
    public void testCheckpointOnDiskFull() throws IOException {
        final Checkpoint checkpoint = randomCheckpoint();
        Path tempDir = createTempDir();
        Checkpoint.write(FileChannel::open, tempDir.resolve("foo.cpk"), checkpoint,
            StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        final Checkpoint checkpoint2 = randomCheckpoint();
        try {
            Checkpoint.write((p, o) -> {
                if (randomBoolean()) {
                    throw new MockDirectoryWrapper.FakeIOException();
                }
                FileChannel open = FileChannel.open(p, o);
                FailSwitch failSwitch = new FailSwitch();
                failSwitch.failNever(); // don't fail in the ctor
                ThrowingFileChannel channel = new ThrowingFileChannel(failSwitch, false, false, open);
                failSwitch.failAlways();
                return channel;

            }, tempDir.resolve("foo.cpk"), checkpoint2, StandardOpenOption.WRITE);
            fail("should have failed earlier");
        } catch (MockDirectoryWrapper.FakeIOException ex) {
            //fine
        }
        Checkpoint read = Checkpoint.read(tempDir.resolve("foo.cpk"));
        assertThat(checkpoint).isEqualTo(read);
    }

    /**
     * Tests that closing views after the translog is fine and we can reopen the translog
     */
    @Test
    public void testPendingDelete() throws IOException {
        translog.add(new Translog.Index("1", 0, primaryTerm.get(), new byte[]{1}));
        translog.rollGeneration();
        TranslogConfig config = translog.getConfig();
        final String translogUUID = translog.getTranslogUUID();
        final TranslogDeletionPolicy deletionPolicy = createTranslogDeletionPolicy(config.getIndexSettings());
        translog.close();
        translog = new Translog(config, translogUUID, deletionPolicy, () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTerm::get,
            seqNo -> {});
        translog.add(new Translog.Index("2", 1, primaryTerm.get(), new byte[]{2}));
        translog.rollGeneration();
        Closeable lock = translog.acquireRetentionLock();
        translog.add(new Translog.Index("3", 2, primaryTerm.get(), new byte[]{3}));
        translog.close();
        IOUtils.close(lock);
        translog = new Translog(config, translogUUID, deletionPolicy, () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTerm::get,
            seqNo -> {});
    }

    public static Translog.Location randomTranslogLocation() {
        return new Translog.Location(randomLong(), randomLong(), randomInt());
    }

    @Test
    public void testTranslogOpSerialization() throws Exception {
        BytesReference B_1 = new BytesArray(new byte[]{1});
        SequenceIDFields seqID = SequenceIDFields.emptySeqID();
        long randomSeqNum = randomNonNegativeLong();
        long randomPrimaryTerm = randomBoolean() ? 0 : randomNonNegativeLong();
        seqID.seqNo.setLongValue(randomSeqNum);
        seqID.seqNoDocValue.setLongValue(randomSeqNum);
        seqID.primaryTerm.setLongValue(randomPrimaryTerm);
        Field idField = new Field(SysColumns.ID.COLUMN.name(), Uid.encodeId("1"), SysColumns.ID.FIELD_TYPE);
        Field versionField = new NumericDocValuesField("_version", 1);
        Document document = new Document();
        document.add(new TextField("value", "test", Field.Store.YES));
        document.add(idField);
        document.add(versionField);
        document.add(seqID.seqNo);
        document.add(seqID.seqNoDocValue);
        document.add(seqID.primaryTerm);
        ParsedDocument doc = new ParsedDocument(versionField, seqID, "1", document, B_1);

        Engine.Index eIndex = new Engine.Index(newUid(doc), doc, randomSeqNum, randomPrimaryTerm,
            1, VersionType.INTERNAL, Origin.PRIMARY, 0, 0, false, SequenceNumbers.UNASSIGNED_SEQ_NO, 0);
        Engine.IndexResult eIndexResult = new Engine.IndexResult(1, randomPrimaryTerm, randomSeqNum, true);
        Translog.Index index = new Translog.Index(eIndex, eIndexResult);

        Version wireVersion = VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumCompatibilityVersion(), Version.CURRENT);
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(wireVersion);
        Translog.Operation.writeOperation(out, index);
        StreamInput in = out.bytes().streamInput();
        in.setVersion(wireVersion);
        Translog.Index serializedIndex = (Translog.Index) Translog.Operation.readOperation(in);
        assertThat(serializedIndex).isEqualTo(index);

        Engine.Delete eDelete = new Engine.Delete(doc.id(), newUid(doc), randomSeqNum, randomPrimaryTerm,
            2, VersionType.INTERNAL, Origin.PRIMARY, 0, SequenceNumbers.UNASSIGNED_SEQ_NO, 0);
        Engine.DeleteResult eDeleteResult = new Engine.DeleteResult(2, randomPrimaryTerm, randomSeqNum, true);
        Translog.Delete delete = new Translog.Delete(eDelete, eDeleteResult);

        out = new BytesStreamOutput();
        out.setVersion(wireVersion);
        Translog.Operation.writeOperation(out, delete);
        in = out.bytes().streamInput();
        in.setVersion(wireVersion);
        Translog.Delete serializedDelete = (Translog.Delete) Translog.Operation.readOperation(in);
        assertThat(serializedDelete).isEqualTo(delete);
    }

    @Test
    public void testRollGeneration() throws Exception {
        // make sure we keep some files around
        final boolean longRetention = randomBoolean();
        final TranslogDeletionPolicy deletionPolicy = translog.getDeletionPolicy();
        if (longRetention) {
            deletionPolicy.setRetentionAgeInMillis(3600 * 1000);
        } else {
            deletionPolicy.setRetentionAgeInMillis(-1);
        }
        // we control retention via time, disable size based calculations for simplicity
        deletionPolicy.setRetentionSizeInBytes(-1);
        final long generation = translog.currentFileGeneration();
        final int rolls = randomIntBetween(1, 16);
        int totalOperations = 0;
        int seqNo = 0;
        final List<Long> primaryTerms = new ArrayList<>();
        primaryTerms.add(primaryTerm.get()); // We always create an empty translog.
        primaryTerms.add(primaryTerm.get());
        for (int i = 0; i < rolls; i++) {
            final int operations = randomIntBetween(1, 128);
            for (int j = 0; j < operations; j++) {
                translog.add(new Translog.NoOp(seqNo++, primaryTerm.get(), "test"));
                totalOperations++;
            }
            try (ReleasableLock ignored = translog.writeLock.acquire()) {
                if (randomBoolean()){
                    primaryTerm.incrementAndGet();
                }
                translog.rollGeneration();
                primaryTerms.add(primaryTerm.get());
            }
            assertThat(translog.currentFileGeneration()).isEqualTo(generation + i + 1);
            assertThat(translog.getCurrent().getPrimaryTerm()).isEqualTo(primaryTerm.get());
            assertThat(translog.totalOperations()).isEqualTo(totalOperations);
        }
        for (int i = 0; i <= rolls; i++) {
            assertFileIsPresent(translog, generation + i);
            final List<Long> storedPrimaryTerms = Stream.concat(translog.getReaders().stream(), Stream.of(translog.getCurrent()))
                .map(t -> t.getPrimaryTerm()).collect(Collectors.toList());
            assertThat(storedPrimaryTerms).isEqualTo(primaryTerms);
        }

        final BaseTranslogReader minRetainedReader = randomFrom(
            Stream.concat(translog.getReaders().stream(), Stream.of(translog.getCurrent()))
                .filter(r -> r.getCheckpoint().minSeqNo >= 0)
                .collect(Collectors.toList()));
        int retainedOps = Stream.concat(translog.getReaders().stream(), Stream.of(translog.getCurrent()))
            .filter(r -> r.getCheckpoint().generation >= minRetainedReader.generation)
            .mapToInt(r -> r.getCheckpoint().numOps)
            .sum();
        deletionPolicy.setLocalCheckpointOfSafeCommit(
            randomLongBetween(minRetainedReader.getCheckpoint().minSeqNo, minRetainedReader.getCheckpoint().maxSeqNo) - 1);
        translog.trimUnreferencedReaders();
        assertThat(translog.currentFileGeneration()).isEqualTo(generation + rolls);
        assertThat(translog.stats().getUncommittedOperations()).isEqualTo(retainedOps);
        if (longRetention) {
            for (int i = 0; i <= rolls; i++) {
                assertFileIsPresent(translog, generation + i);
            }
            deletionPolicy.setRetentionAgeInMillis(randomBoolean() ? 100 : -1);
            assertBusy(() -> {
                translog.trimUnreferencedReaders();
                for (long i = 0; i < minRetainedReader.generation; i++) {
                    assertFileDeleted(translog, i);
                }
            });
        } else {
            // immediate cleanup
            for (long i = 0; i < minRetainedReader.generation; i++) {
                assertFileDeleted(translog, i);
            }
        }
        for (long i = minRetainedReader.generation; i < generation + rolls; i++) {
            assertFileIsPresent(translog, i);
        }
    }

    @Test
    public void testMinSeqNoBasedAPI() throws IOException {
        final int operations = randomIntBetween(1, 512);
        final List<Long> shuffledSeqNos = LongStream.range(0, operations).boxed().collect(Collectors.toList());
        Randomness.shuffle(shuffledSeqNos);
        final List<Tuple<Long, Long>> seqNos = new ArrayList<>();
        final Map<Long, Long> terms = new HashMap<>();
        for (final Long seqNo : shuffledSeqNos) {
            seqNos.add(new Tuple<>(seqNo, terms.computeIfAbsent(seqNo, k -> 0L)));
            Long repeatingTermSeqNo = randomFrom(seqNos.stream().map(Tuple::v1).collect(Collectors.toList()));
            seqNos.add(new Tuple<>(repeatingTermSeqNo, terms.get(repeatingTermSeqNo)));
        }

        for (final Tuple<Long, Long> tuple : seqNos) {
            translog.add(new Translog.NoOp(tuple.v1(), tuple.v2(), "test"));
            if (rarely()) {
                translog.rollGeneration();
            }
        }

        final Map<Long, Set<Tuple<Long, Long>>> seqNoPerGeneration = new HashMap<>();
        final Map<Long, Integer> opCountPerGeneration = new HashMap<>();
        // one extra roll to make sure that all ops so far are available via a reader and a translog-{gen}.ckp
        // file in a consistent way, in order to simplify checking code.
        translog.rollGeneration();
        for (long seqNo = 0; seqNo < operations; seqNo++) {
            final Set<Tuple<Long, Long>> seenSeqNos = new HashSet<>();
            final long generation = translog.getMinGenerationForSeqNo(seqNo).translogFileGeneration;
            int expectedSnapshotOps = 0;
            for (long g = generation; g < translog.currentFileGeneration(); g++) {
                if (!seqNoPerGeneration.containsKey(g)) {
                    final Set<Tuple<Long, Long>> generationSeenSeqNos = new HashSet<>();
                    int opCount = 0;
                    final Checkpoint checkpoint = Checkpoint.read(translog.location().resolve(Translog.getCommitCheckpointFileName(g)));
                    try (TranslogReader reader = translog.openReader(translog.location().resolve(Translog.getFilename(g)), checkpoint)) {
                        TranslogSnapshot snapshot = reader.newSnapshot();
                        Translog.Operation operation;
                        while ((operation = snapshot.next()) != null) {
                            generationSeenSeqNos.add(new Tuple<>(operation.seqNo(), operation.primaryTerm()));
                            opCount++;
                        }
                        assertThat(opCount).isEqualTo(reader.totalOperations());
                        assertThat(opCount).isEqualTo(checkpoint.numOps);
                    }
                    opCountPerGeneration.put(g, opCount);
                    seqNoPerGeneration.put(g, generationSeenSeqNos);
                }
                final Set<Tuple<Long, Long>> generationSeqNo = seqNoPerGeneration.get(g);
                if (generationSeqNo.stream().map(Tuple::v1).max(Long::compareTo).orElse(Long.MIN_VALUE) >= seqNo) {
                    expectedSnapshotOps += opCountPerGeneration.get(g);
                }
                seenSeqNos.addAll(generationSeqNo);
            }
            assertThat(translog.estimateTotalOperationsFromMinSeq(seqNo)).isEqualTo(expectedSnapshotOps);
            int readFromSnapshot = 0;
            try (Translog.Snapshot snapshot = translog.newSnapshot(seqNo, Long.MAX_VALUE)) {
                assertThat(snapshot.totalOperations()).isEqualTo(expectedSnapshotOps);
                Translog.Operation op;
                while ((op = snapshot.next()) != null) {
                    assertThat(new Tuple<>(op.seqNo(), op.primaryTerm())).isIn(seenSeqNos);
                    readFromSnapshot++;
                }
                readFromSnapshot += snapshot.skippedOperations();
            }
            assertThat(readFromSnapshot).isEqualTo(expectedSnapshotOps);
            final long seqNoLowerBound = seqNo;
            final Set<Tuple<Long, Long>> expected = seqNos.stream().filter(t -> t.v1() >= seqNoLowerBound).collect(Collectors.toSet());
            seenSeqNos.retainAll(expected);
            assertThat(seenSeqNos).isEqualTo(expected);
        }
    }

    @Test
    public void testSimpleCommit() throws IOException {
        final int operations = randomIntBetween(1, 4096);
        long seqNo = 0;
        for (int i = 0; i < operations; i++) {
            translog.add(new Translog.NoOp(seqNo++, primaryTerm.get(), "test'"));
            if (rarely()) {
                if (rarely()) {
                    primaryTerm.incrementAndGet();
                }
                translog.rollGeneration();
            }
        }
        translog.getDeletionPolicy().setLocalCheckpointOfSafeCommit(randomLongBetween(0, operations));
    }

    @Test
    public void testAcquiredLockIsPassedToDeletionPolicy() throws IOException {
        final int operations = randomIntBetween(1, 4096);
        final TranslogDeletionPolicy deletionPolicy = translog.getDeletionPolicy();
        for (int i = 0; i < operations; i++) {
            translog.add(new Translog.NoOp(i, 0, "test"));
            if (rarely()) {
                translog.rollGeneration();
            }
            if (rarely()) {
                translog.getDeletionPolicy().setLocalCheckpointOfSafeCommit(
                    randomLongBetween(deletionPolicy.getLocalCheckpointOfSafeCommit(), i));
            }
            if (frequently()) {
                long minGen;
                try (Closeable ignored = translog.acquireRetentionLock()) {
                    minGen = translog.getMinFileGeneration();
                    assertThat(deletionPolicy.getTranslogRefCount(minGen)).isEqualTo(1L);
                }
                assertThat(deletionPolicy.getTranslogRefCount(minGen)).isEqualTo(0L);
            }
        }
    }

    @Test
    public void testReadGlobalCheckpoint() throws Exception {
        final String translogUUID = translog.getTranslogUUID();
        globalCheckpoint.set(randomNonNegativeLong());
        final int operations = randomIntBetween(1, 100);
        for (int i = 0; i < operations; i++) {
            translog.add(new Translog.NoOp(randomNonNegativeLong(), 0, "test'"));
            if (rarely()) {
                translog.rollGeneration();
            }
        }
        translog.rollGeneration();
        translog.close();
        assertThat(Translog.readGlobalCheckpoint(translogDir, translogUUID)).isEqualTo(globalCheckpoint.get());
        assertThatThrownBy(() -> Translog.readGlobalCheckpoint(translogDir, UUIDs.randomBase64UUID()))
            .isExactlyInstanceOf(TranslogCorruptedException.class);
    }

    @Test
    public void testSnapshotReadOperationInReverse() throws Exception {
        final Deque<List<Translog.Operation>> views = new ArrayDeque<>();
        views.push(new ArrayList<>());
        final AtomicLong seqNo = new AtomicLong();

        final int generations = randomIntBetween(2, 20);
        for (int gen = 0; gen < generations; gen++) {
            final int operations = randomIntBetween(1, 100);
            for (int i = 0; i < operations; i++) {
                Translog.Index op = new Translog.Index(randomAlphaOfLength(10),
                    seqNo.getAndIncrement(), primaryTerm.get(), new byte[]{1});
                translog.add(op);
                views.peek().add(op);
            }
            if (frequently()) {
                translog.rollGeneration();
                views.push(new ArrayList<>());
            }
        }
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            final List<Translog.Operation> expectedSeqNo = new ArrayList<>();
            while (views.isEmpty() == false) {
                expectedSeqNo.addAll(views.pop());
            }
            assertThat(snapshot).equalsTo(expectedSeqNo);
        }
    }

    @Test
    public void testSnapshotDedupOperations() throws Exception {
        final Map<Long, Translog.Operation> latestOperations = new HashMap<>();
        final int generations = between(2, 20);
        for (int gen = 0; gen < generations; gen++) {
            List<Long> batch = LongStream.rangeClosed(0, between(0, 500)).boxed().collect(Collectors.toList());
            Randomness.shuffle(batch);
            for (Long seqNo : batch) {
                Translog.Index op = new Translog.Index(
                    randomAlphaOfLength(10), seqNo, primaryTerm.get(), new byte[]{1});
                translog.add(op);
                latestOperations.put(op.seqNo(), op);
            }
            translog.rollGeneration();
        }
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertThat(snapshot).containsOperationsInAnyOrder(latestOperations.values());
        }
    }

    /** Make sure that it's ok to close a translog snapshot multiple times */
    @Test
    public void testCloseSnapshotTwice() throws Exception {
        int numOps = between(0, 10);
        for (int i = 0; i < numOps; i++) {
            Translog.Index op = new Translog.Index(randomAlphaOfLength(10), i, primaryTerm.get(), new byte[]{1});
            translog.add(op);
            if (randomBoolean()) {
                translog.rollGeneration();
            }
        }
        for (int i = 0; i < 5; i++) {
            Translog.Snapshot snapshot = translog.newSnapshot();
           assertThat(snapshot).hasSize(numOps);
            snapshot.close();
            snapshot.close();
        }
    }

    // close method should never be called directly from Translog (the only exception is closeOnTragicEvent)
    @Test
    public void testTranslogCloseInvariant() throws IOException {
        assumeTrue("test only works with assertions enabled", Assertions.ENABLED);
        class MisbehavingTranslog extends Translog {
            MisbehavingTranslog(TranslogConfig config, String translogUUID, TranslogDeletionPolicy deletionPolicy,
                                    LongSupplier globalCheckpointSupplier, LongSupplier primaryTermSupplier) throws IOException {
                super(config, translogUUID, deletionPolicy, globalCheckpointSupplier, primaryTermSupplier, seqNo -> {});
            }

            void callCloseDirectly() throws IOException {
                close();
            }

            void callCloseUsingIOUtilsWithExceptionHandling() {
                IOUtils.closeWhileHandlingException(this);
            }

            void callCloseUsingIOUtils() throws IOException {
                IOUtils.close(this);
            }

            void callCloseOnTragicEvent() {
                Exception e = new Exception("test tragic exception");
                tragedy.setTragicException(e);
                closeOnTragicEvent(e);
            }
        }


        globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        Path path = createTempDir();
        final TranslogConfig translogConfig = getTranslogConfig(path);
        final TranslogDeletionPolicy deletionPolicy = createTranslogDeletionPolicy(translogConfig.getIndexSettings());
        final String translogUUID = Translog.createEmptyTranslog(path, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        MisbehavingTranslog misbehavingTranslog = new MisbehavingTranslog(translogConfig, translogUUID, deletionPolicy,
            () -> globalCheckpoint.get(), primaryTerm::get);

        assertThatThrownBy(() -> misbehavingTranslog.callCloseDirectly())
            .isExactlyInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> misbehavingTranslog.callCloseUsingIOUtils())
            .isExactlyInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> misbehavingTranslog.callCloseUsingIOUtilsWithExceptionHandling())
            .isExactlyInstanceOf(AssertionError.class);
        misbehavingTranslog.callCloseOnTragicEvent();
    }

    @Test
    public void testMaxSeqNo() throws Exception {
        Map<Long, Long> maxSeqNoPerGeneration = new HashMap<>();
        for (int iterations = between(1, 10), i = 0; i < iterations; i++) {
            long startSeqNo = randomLongBetween(0, Integer.MAX_VALUE);
            List<Long> seqNos = LongStream.range(startSeqNo, startSeqNo + randomInt(100)).boxed().collect(Collectors.toList());
            Randomness.shuffle(seqNos);
            for (long seqNo : seqNos) {
                if (frequently()) {
                    translog.add(new Translog.Index("id", seqNo, primaryTerm.get(), new byte[]{1}));
                    maxSeqNoPerGeneration.compute(translog.currentFileGeneration(),
                        (key, existing) -> existing == null ? seqNo : Math.max(existing, seqNo));
                }
            }
            translog.rollGeneration();
        }
        translog.sync();
        long expected = maxSeqNoPerGeneration.isEmpty() ? SequenceNumbers.NO_OPS_PERFORMED : Collections.max(maxSeqNoPerGeneration.values());
        assertThat(translog.getMaxSeqNo()).isEqualTo(expected);
        long expectedMaxSeqNo = maxSeqNoPerGeneration.entrySet().stream()
            .filter(e -> e.getKey() >= translog.getMinFileGeneration()).mapToLong(e -> e.getValue())
            .max().orElse(SequenceNumbers.NO_OPS_PERFORMED);
        assertThat(translog.getMaxSeqNo()).isEqualTo(expectedMaxSeqNo);
    }

    static class SortedSnapshot implements Translog.Snapshot {
        private final Translog.Snapshot snapshot;
        private List<Translog.Operation> operations = null;

        SortedSnapshot(Translog.Snapshot snapshot) {
            this.snapshot = snapshot;
        }

        @Override
        public int totalOperations() {
            return snapshot.totalOperations();
        }

        @Override
        public Translog.Operation next() throws IOException {
            if (operations == null) {
                operations = new ArrayList<>();
                Translog.Operation op;
                while ((op = snapshot.next()) != null) {
                    operations.add(op);
                }
                operations.sort(Comparator.comparing(Translog.Operation::seqNo));
            }
            if (operations.isEmpty()) {
                return null;
            }
            return operations.remove(0);
        }

        @Override
        public void close() throws IOException {
            snapshot.close();
        }
    }

    @Test
    public void testCrashDuringCheckpointCopy() throws IOException {
        final Path path = createTempDir();
        final AtomicBoolean failOnCopy = new AtomicBoolean();
        final String expectedExceptionMessage = "simulated failure during copy";
        final FilterFileSystemProvider filterFileSystemProvider
            = new FilterFileSystemProvider(path.getFileSystem().provider().getScheme(), path.getFileSystem()) {

            @Override
            public void copy(Path source, Path target, CopyOption... options) throws IOException {
                if (failOnCopy.get() && source.toString().endsWith(Translog.CHECKPOINT_SUFFIX)) {
                    deleteIfExists(target);
                    Files.createFile(target);
                    throw new IOException(expectedExceptionMessage);
                } else {
                    super.copy(source, target, options);
                }
            }
        };

        try (Translog brokenTranslog = create(filterFileSystemProvider.getPath(path.toUri()))) {
            failOnCopy.set(true);
            primaryTerm.incrementAndGet(); // increment primary term to force rolling generation
            assertThatThrownBy(brokenTranslog::rollGeneration)
                .isExactlyInstanceOf(IOException.class)
                .hasMessage(expectedExceptionMessage);
            assertThat(brokenTranslog.isOpen()).isFalse();

            try (Translog recoveredTranslog = new Translog(getTranslogConfig(path), brokenTranslog.getTranslogUUID(),
                brokenTranslog.getDeletionPolicy(), () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTerm::get, seqNo -> {})) {
                recoveredTranslog.rollGeneration();
                assertFilePresences(recoveredTranslog);
            }
        }
    }

    @Test
    public void testSyncConcurrently() throws Exception {
        Path path = createTempDir("translog");
        TranslogConfig config = getTranslogConfig(path);
        String translogUUID = Translog.createEmptyTranslog(
            config.getTranslogPath(), SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        Set<Long> persistedSeqNos = Sets.newConcurrentHashSet();
        AtomicLong lastGlobalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        LongSupplier globalCheckpointSupplier = () -> {
            if (randomBoolean()) {
                return lastGlobalCheckpoint.addAndGet(randomIntBetween(1, 100));
            } else {
                return lastGlobalCheckpoint.get();
            }
        };
        try (Translog translog = new Translog(config, translogUUID, createTranslogDeletionPolicy(config.getIndexSettings()),
            globalCheckpointSupplier, primaryTerm::get, persistedSeqNos::add)) {
            Thread[] threads = new Thread[between(2, 8)];
            Phaser phaser = new Phaser(threads.length);
            AtomicLong nextSeqNo = new AtomicLong();
            for (int t = 0; t < threads.length; t++) {
                threads[t] = new Thread(() -> {
                    phaser.arriveAndAwaitAdvance();
                    int iterations = randomIntBetween(10, 100);
                    for (int i = 0; i < iterations; i++) {
                        List<Translog.Operation> ops = IntStream.range(0, between(1, 10))
                            .mapToObj(n -> new Translog.Index("1", nextSeqNo.incrementAndGet(), primaryTerm.get(), new byte[]{1}))
                            .collect(Collectors.toList());
                        try {
                            Translog.Location location = null;
                            for (Translog.Operation op : ops) {
                                location = translog.add(op);
                            }
                            assertThat(location).isNotNull();
                            long globalCheckpoint = lastGlobalCheckpoint.get();
                            final boolean synced;
                            if (randomBoolean()) {
                                synced = translog.ensureSynced(location);
                            } else {
                                translog.sync();
                                synced = true;
                            }
                            for (Translog.Operation op : ops) {
                                assertThat(persistedSeqNos)
                                    .as("seq# " + op.seqNo() + " was not marked as persisted")
                                    .contains(op.seqNo());
                            }
                            Checkpoint checkpoint = translog.getLastSyncedCheckpoint();
                            assertThat(checkpoint.offset).isGreaterThanOrEqualTo(location.translogLocation);
                            for (Translog.Operation op : ops) {
                                assertThat(checkpoint.minSeqNo).isLessThanOrEqualTo(op.seqNo());
                                assertThat(checkpoint.maxSeqNo).isGreaterThanOrEqualTo(op.seqNo());
                            }
                            if (synced) {
                                assertThat(checkpoint.globalCheckpoint).isGreaterThanOrEqualTo(globalCheckpoint);
                            }
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
                    }
                });
                threads[t].start();
            }
            for (Thread thread : threads) {
                thread.join();
            }
        }
    }

    @Test
    public void testEnsureNoCircularException() throws Exception {
        final AtomicBoolean failedToSyncCheckpoint = new AtomicBoolean();
        final ChannelFactory channelFactory = (file, openOption) -> {
            final FileChannel channel = FileChannel.open(file, openOption);
            return new FilterFileChannel(channel) {
                @Override
                public void force(boolean metaData) throws IOException {
                    if (failedToSyncCheckpoint.get()) {
                        throw new IOException("simulated");
                    }
                    super.force(metaData);
                }
            };
        };
        final TranslogConfig config = getTranslogConfig(createTempDir());
        final String translogUUID = Translog.createEmptyTranslog(
            config.getTranslogPath(), SequenceNumbers.NO_OPS_PERFORMED, shardId, channelFactory, primaryTerm.get());
        final Translog translog = new Translog(
            config,
            translogUUID,
            createTranslogDeletionPolicy(config.getIndexSettings()),
            () -> SequenceNumbers.NO_OPS_PERFORMED,
            primaryTerm::get,
            seqNo -> {})
        {
            @Override
            ChannelFactory getChannelFactory() {
                return channelFactory;
            }

            @Override
            void syncBeforeRollGeneration() {
                // make it a noop like the old versions
            }
        };
        try (translog) {
            translog.add(new Translog.Index("1", 1, primaryTerm.get(), new byte[]{1}));
            failedToSyncCheckpoint.set(true);
            assertThatThrownBy(translog::rollGeneration)
                .isExactlyInstanceOf(IOException.class);
            assertThatThrownBy(translog::rollGeneration)
                .isExactlyInstanceOf(AlreadyClosedException.class)
                .satisfies(ex -> assertThat(hasCircularReference(ex)).isFalse());
        }
    }

    @Test
    public void test_translog_index_operation_bwc_serialization() throws Throwable {
        Translog.Index index = new Translog.Index("id1", 2L, 1L, new byte[] { 1, 2, 3, 4 });
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.CURRENT.minimumIndexCompatibilityVersion());
        index.write(out);
        StreamInput in = out.bytes().streamInput();

        Translog.Index index2 = new Translog.Index(in);
        assertThat(index.id()).isEqualTo(index2.id());
        assertThat(index.version()).isEqualTo(index2.version());
        assertThat(index.seqNo()).isEqualTo(index2.seqNo());
        assertThat(index.primaryTerm()).isEqualTo(index2.primaryTerm());
        assertThat(index.getSource()).isEqualTo(index2.getSource());
    }

    static boolean hasCircularReference(Throwable cause) {
        final Queue<Throwable> queue = new LinkedList<>();
        queue.add(cause);
        final Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        while (queue.isEmpty() == false) {
            final Throwable current = queue.remove();
            if (seen.add(current) == false) {
                return true;
            }
            Collections.addAll(queue, current.getSuppressed());
            if (current.getCause() != null) {
                queue.add(current.getCause());
            }
        }
        return false;
    }
}
