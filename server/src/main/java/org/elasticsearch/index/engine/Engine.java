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

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndVersion;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStats;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.common.exceptions.Exceptions;
import io.crate.common.unit.TimeValue;
import io.crate.exceptions.SQLExceptions;
import io.crate.expression.reference.doc.lucene.StoredRowLookup;
import io.crate.metadata.doc.SysColumns;

public abstract class Engine implements Closeable {

    public static final String SYNC_COMMIT_ID = "sync_id";
    public static final String HISTORY_UUID_KEY = "history_uuid";
    public static final String FORCE_MERGE_UUID_KEY = "force_merge_uuid";
    public static final String MIN_RETAINED_SEQNO = "min_retained_seq_no";
    public static final String MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID = "max_unsafe_auto_id_timestamp";

    protected final ShardId shardId;
    protected final Logger logger;
    protected final EngineConfig engineConfig;
    protected final Store store;
    protected final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final CountDownLatch closedLatch = new CountDownLatch(1);
    protected final EventListener eventListener;
    protected final ReentrantLock failEngineLock = new ReentrantLock();
    protected final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    protected final ReleasableLock readLock = new ReleasableLock(rwl.readLock());
    protected final ReleasableLock writeLock = new ReleasableLock(rwl.writeLock());
    protected final SetOnce<Exception> failedEngine = new SetOnce<>();
    /*
     * on {@code lastWriteNanos} we use System.nanoTime() to initialize this since:
     *  - we use the value for figuring out if the shard / engine is active so if we startup and no write has happened yet we still consider it active
     *    for the duration of the configured active to inactive period. If we initialize to 0 or Long.MAX_VALUE we either immediately or never mark it
     *    inactive if no writes at all happen to the shard.
     *  - we also use this to flush big-ass merges on an inactive engine / shard but if we we initialize 0 or Long.MAX_VALUE we either immediately or never
     *    commit merges even though we shouldn't from a user perspective (this can also have funky sideeffects in tests when we open indices with lots of segments
     *    and suddenly merges kick in.
     *  NOTE: don't use this value for anything accurate it's a best effort for freeing up diskspace after merges and on a shard level to reduce index buffer sizes on
     *  inactive shards.
     *
     *  We also store the startTimeMillis to have the initial real timestamp of the engine start and
     *  startTimeNanos to be able to calculate elapsed time in nanos
     */
    private long startTimeMillis = System.currentTimeMillis();
    private long startTimeNanos = System.nanoTime();
    protected volatile long lastWriteNanos = startTimeNanos;


    protected Engine(EngineConfig engineConfig) {
        Objects.requireNonNull(engineConfig.getStore(), "Store must be provided to the engine");

        this.engineConfig = engineConfig;
        this.shardId = engineConfig.getShardId();
        this.store = engineConfig.getStore();
        this.logger = Loggers.getLogger(Engine.class, // we use the engine class directly here to make sure all subclasses have the same logger name
                engineConfig.getShardId());
        this.eventListener = engineConfig.getEventListener();
    }

    public final EngineConfig config() {
        return engineConfig;
    }

    protected abstract SegmentInfos getLastCommittedSegmentInfos();

    /** returns the history uuid for the engine */
    public abstract String getHistoryUUID();

    /** Returns how many bytes we are currently moving from heap to disk */
    public abstract long getWritingBytes();

    /**
     * Returns the {@link DocsStats} for this engine
     */
    public DocsStats docStats() {
        // we calculate the doc stats based on the internal searcher that is more up-to-date and not subject
        // to external refreshes. For instance we don't refresh an external searcher if we flush and indices with
        // index.refresh_interval=-1 won't see any doc stats updates at all. This change will give more accurate statistics
        // when indexing but not refreshing in general. Yet, if a refresh happens the internal searcher is refresh as well so we are
        // safe here.
        try (Searcher searcher = acquireSearcher("docStats", SearcherScope.INTERNAL)) {
            return docsStats(searcher.getIndexReader());
        }
    }

    protected final DocsStats docsStats(IndexReader indexReader) {
        long numDocs = 0;
        long numDeletedDocs = 0;
        long sizeInBytes = 0;
        // we don't wait for a pending refreshes here since it's a stats call instead we mark it as accessed only which will cause
        // the next scheduled refresh to go through and refresh the stats as well
        for (LeafReaderContext readerContext : indexReader.leaves()) {
            // we go on the segment level here to get accurate numbers
            final SegmentReader segmentReader = Lucene.segmentReader(readerContext.reader());
            SegmentCommitInfo info = segmentReader.getSegmentInfo();
            numDocs += readerContext.reader().numDocs();
            numDeletedDocs += readerContext.reader().numDeletedDocs();
            try {
                sizeInBytes += info.sizeInBytes();
            } catch (IOException e) {
                logger.trace(() -> new ParameterizedMessage("failed to get size for [{}]", info.info.name), e);
            }
        }
        return new DocsStats(numDocs, numDeletedDocs, sizeInBytes);
    }

    /**
     * Performs the pre-closing checks on the {@link Engine}.
     *
     * @throws IllegalStateException if the sanity checks failed
     */
    public void verifyEngineBeforeIndexClosing() throws IllegalStateException {
        final long globalCheckpoint = engineConfig.getGlobalCheckpointSupplier().getAsLong();
        final long maxSeqNo = getSeqNoStats(globalCheckpoint).getMaxSeqNo();
        if (globalCheckpoint != maxSeqNo) {
            throw new IllegalStateException("Global checkpoint [" + globalCheckpoint
                + "] mismatches maximum sequence number [" + maxSeqNo + "] on index shard " + shardId);
        }
    }

    /**
     * A throttling class that can be activated, causing the
     * {@code acquireThrottle} method to block on a lock when throttling
     * is enabled
     */
    protected static final class IndexThrottle {
        private volatile long startOfThrottleNS;
        private static final ReleasableLock NOOP_LOCK = new ReleasableLock(new NoOpLock());
        private final ReleasableLock lockReference = new ReleasableLock(new ReentrantLock());
        private volatile ReleasableLock lock = NOOP_LOCK;

        public Releasable acquireThrottle() {
            return lock.acquire();
        }

        /** Activate throttling, which switches the lock to be a real lock */
        public void activate() {
            assert lock == NOOP_LOCK : "throttling activated while already active";
            startOfThrottleNS = System.nanoTime();
            lock = lockReference;
        }

        /** Deactivate throttling, which switches the lock to be an always-acquirable NoOpLock */
        public void deactivate() {
            assert lock != NOOP_LOCK : "throttling deactivated but not active";
            lock = NOOP_LOCK;

            assert startOfThrottleNS > 0 : "Bad state of startOfThrottleNS";
        }

        boolean isThrottled() {
            return lock != NOOP_LOCK;
        }

        boolean throttleLockIsHeldByCurrentThread() { // to be used in assertions and tests only
            if (isThrottled()) {
                return lock.isHeldByCurrentThread();
            }
            return false;
        }
    }

    /**
     * Trims translog for terms below <code>belowTerm</code> and seq# above <code>aboveSeqNo</code>
     * @see Translog#trimOperations(long, long)
     */
    public abstract void trimOperationsFromTranslog(long belowTerm, long aboveSeqNo) throws EngineException;

    /** A Lock implementation that always allows the lock to be acquired */
    protected static final class NoOpLock implements Lock {

        @Override
        public void lock() {
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
        }

        @Override
        public boolean tryLock() {
            return true;
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            return true;
        }

        @Override
        public void unlock() {
        }

        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException("NoOpLock can't provide a condition");
        }
    }

    /**
     * Perform document index operation on the engine
     * @param index operation to perform
     * @return {@link IndexResult} containing updated translog location, version and
     * document specific failures
     *
     * Note: engine level failures (i.e. persistent engine failures) are thrown
     */
    public abstract IndexResult index(Index index) throws IOException;

    /**
     * Perform document delete operation on the engine
     * @param delete operation to perform
     * @return {@link DeleteResult} containing updated translog location, version and
     * document specific failures
     *
     * Note: engine level failures (i.e. persistent engine failures) are thrown
     */
    public abstract DeleteResult delete(Delete delete) throws IOException;

    public abstract NoOpResult noOp(NoOp noOp) throws IOException;

    /**
     * Base class for index and delete operation results
     * Holds result meta data (e.g. translog location, updated version)
     * for an executed write {@link Operation}
     **/
    public abstract static class Result {
        private final Result.Type resultType;
        private final long version;
        private final long term;
        private final long seqNo;
        private final Exception failure;
        private final SetOnce<Boolean> freeze = new SetOnce<>();
        private Translog.Location translogLocation;

        protected Result(Exception failure, long version, long term, long seqNo) {
            this.failure = Objects.requireNonNull(failure);
            this.version = version;
            this.term = term;
            this.seqNo = seqNo;
            this.resultType = Type.FAILURE;
        }

        protected Result(long version, long term, long seqNo) {
            this.version = version;
            this.seqNo = seqNo;
            this.term = term;
            this.failure = null;
            this.resultType = Type.SUCCESS;
        }

        protected Result() {
            this.version = Versions.NOT_FOUND;
            this.seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
            this.term = 0L;
            this.failure = null;
            this.resultType = Type.MAPPING_UPDATE_REQUIRED;
        }

        /** whether the operation was successful, has failed or was aborted due to a mapping update */
        public Type getResultType() {
            return resultType;
        }

        /** get the updated document version */
        public long getVersion() {
            return version;
        }

        /**
         * Get the sequence number on the primary.
         *
         * @return the sequence number
         */
        public long getSeqNo() {
            return seqNo;
        }

        public long getTerm() {
            return term;
        }

        /** get the translog location after executing the operation */
        public Translog.Location getTranslogLocation() {
            return translogLocation;
        }

        /** get document failure while executing the operation {@code null} in case of no failure */
        public Exception getFailure() {
            return failure;
        }

        @VisibleForTesting
        public void setTranslogLocation(Translog.Location translogLocation) {
            if (freeze.get() == null) {
                this.translogLocation = translogLocation;
            } else {
                throw new IllegalStateException("result is already frozen");
            }
        }

        void freeze() {
            freeze.set(true);
        }

        public enum Type {
            SUCCESS,
            FAILURE,
            MAPPING_UPDATE_REQUIRED
        }
    }

    public static class IndexResult extends Result {

        private final boolean created;

        public IndexResult(long version, long term, long seqNo, boolean created) {
            super(version, term, seqNo);
            this.created = created;
        }

        /**
         * use in case of the index operation failed before getting to internal engine
         **/
        public IndexResult(Exception failure, long version) {
            this(failure, version, UNASSIGNED_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_SEQ_NO);
        }

        public IndexResult(Exception failure, long version, long term, long seqNo) {
            super(failure, version, term, seqNo);
            this.created = false;
        }

        public IndexResult() {
            super();
            this.created = false;
        }

        public boolean isCreated() {
            return created;
        }

    }

    public static class DeleteResult extends Result {

        private final boolean found;

        public DeleteResult(long version, long term, long seqNo, boolean found) {
            super(version, term, seqNo);
            this.found = found;
        }

        /**
         * use in case of the delete operation failed before getting to internal engine
         **/
        public DeleteResult(Exception failure, long version, long term) {
            this(failure, version, term, SequenceNumbers.UNASSIGNED_SEQ_NO, false);
        }

        public DeleteResult(Exception failure, long version, long term, long seqNo, boolean found) {
            super(failure, version, term, seqNo);
            this.found = found;
        }

        public boolean isFound() {
            return found;
        }

    }

    public static class NoOpResult extends Result {

        NoOpResult(long term, long seqNo) {
            super(0, term, seqNo);
        }

        NoOpResult(long term, long seqNo, Exception failure) {
            super(failure, 0, term, seqNo);
        }

    }

    /**
     * Attempts to do a special commit where the given syncID is put into the commit data. The attempt
     * succeeds if there are not pending writes in lucene and the current point is equal to the expected one.
     *
     * @param syncId           id of this sync
     * @param expectedCommitId the expected value of
     * @return true if the sync commit was made, false o.w.
     */
    public abstract SyncedFlushResult syncFlush(String syncId, CommitId expectedCommitId) throws EngineException;

    public enum SyncedFlushResult {
        SUCCESS,
        COMMIT_MISMATCH,
        PENDING_OPERATIONS
    }

    protected final GetResult getFromSearcher(Get get, BiFunction<String, SearcherScope, Searcher> searcherFactory,
                                              SearcherScope scope) throws EngineException {
        final Engine.Searcher searcher = searcherFactory.apply("get", scope);
        final DocIdAndVersion docIdAndVersion;
        try {
            docIdAndVersion = VersionsAndSeqNoResolver.loadDocIdAndVersion(searcher.getIndexReader(), get.uid(), true);
        } catch (Exception e) {
            Releasables.closeIgnoringException(searcher);
            //TODO: A better exception goes here
            throw new EngineException(shardId, "Couldn't resolve version", e);
        }

        if (docIdAndVersion != null) {
            if (get.versionType().isVersionConflictForReads(docIdAndVersion.version, get.version())) {
                Releasables.close(searcher);
                throw new VersionConflictEngineException(
                    shardId,
                    get.id(),
                    get.versionType().explainConflictForReads(docIdAndVersion.version, get.version())
                );
            }
            if (get.getIfSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO && (
                get.getIfSeqNo() != docIdAndVersion.seqNo || get.getIfPrimaryTerm() != docIdAndVersion.primaryTerm)) {

                Releasables.close(searcher);
                throw new VersionConflictEngineException(
                    shardId,
                    get.id(),
                    get.getIfSeqNo(),
                    get.getIfPrimaryTerm(),
                    docIdAndVersion.seqNo,
                    docIdAndVersion.primaryTerm
                );
            }
        }

        if (docIdAndVersion != null) {
            // don't release the searcher on this path, it is the
            // responsibility of the caller to call GetResult.release
            return new GetResult(docIdAndVersion, searcher);
        } else {
            Releasables.close(searcher);
            return GetResult.NOT_EXISTS;
        }
    }

    public abstract GetResult get(Get get, BiFunction<String, SearcherScope, Searcher> searcherFactory) throws EngineException;


    /**
     * Returns a new searcher instance. The consumer of this
     * API is responsible for releasing the returned searcher in a
     * safe manner, preferably in a try/finally block.
     *
     * @param source the source API or routing that triggers this searcher acquire
     *
     * @see Searcher#close()
     */
    public final Searcher acquireSearcher(String source) throws EngineException {
        return acquireSearcher(source, SearcherScope.EXTERNAL);
    }

    /**
     * Returns a new searcher instance. The consumer of this
     * API is responsible for releasing the returned searcher in a
     * safe manner, preferably in a try/finally block.
     *
     * @param source the source API or routing that triggers this searcher acquire
     * @param scope the scope of this searcher ie. if the searcher will be used for get or search purposes
     *
     * @see Searcher#close()
     */
    public Searcher acquireSearcher(String source, SearcherScope scope) throws EngineException {
        /* Acquire order here is store -> manager since we need
         * to make sure that the store is not closed before
         * the searcher is acquired. */
        if (store.tryIncRef() == false) {
            throw new AlreadyClosedException(shardId + " store is closed", failedEngine.get());
        }
        Releasable releasable = store::decRef;
        try {
            ReferenceManager<ElasticsearchDirectoryReader> referenceManager = getReferenceManager(scope);
            final ElasticsearchDirectoryReader acquire = referenceManager.acquire();
            AtomicBoolean released = new AtomicBoolean(false);
            Searcher engineSearcher = new Searcher(
                source,
                acquire,
                engineConfig.getQueryCache(),
                engineConfig.getQueryCachingPolicy(),
                () -> {
                    if (released.compareAndSet(false, true)) {
                        try {
                            referenceManager.release(acquire);
                        } finally {
                            store.decRef();
                        }
                    } else {
                        /*
                            * In general, readers should never be released twice or this would break
                            * reference counting. There is one rare case when it might happen though: when
                            * the request and the Reaper thread would both try to release it in a very
                            * short amount of time, this is why we only log a warning instead of throwing
                            * an exception.
                            */
                        logger.warn("Searcher was released twice", new IllegalStateException("Double release"));
                    }
                }
            );
            releasable = null; // success - hand over the reference to the engine reader
            return engineSearcher;
        } catch (AlreadyClosedException ex) {
            throw ex;
        } catch (Exception ex) {
            maybeFailEngine("acquire_searcher", ex);
            ensureOpen(ex); // throw EngineCloseException here if we are already closed
            logger.error(() -> new ParameterizedMessage("failed to acquire searcher, source {}", source), ex);
            throw new EngineException(shardId, "failed to acquire searcher, source " + source, ex);
        } finally {
            Releasables.close(releasable);
        }
    }

    protected abstract ReferenceManager<ElasticsearchDirectoryReader> getReferenceManager(SearcherScope scope);

    public enum SearcherScope {
        EXTERNAL, INTERNAL
    }

    /**
     * Checks if the underlying storage sync is required.
     */
    public abstract boolean isTranslogSyncNeeded();

    /**
     * Ensures that all locations in the given stream have been written to the underlying storage.
     */
    public abstract boolean ensureTranslogSynced(Stream<Translog.Location> locations) throws IOException;

    public abstract void syncTranslog() throws IOException;

    /**
     * Acquires a lock on the translog files and Lucene soft-deleted documents to prevent them from being trimmed
     */
    public abstract Closeable acquireHistoryRetentionLock(HistorySource historySource);

    /**
     * Creates a new history snapshot from Lucene for reading operations whose seqno in the requesting seqno range (both inclusive)
     */
    public abstract Translog.Snapshot newChangesSnapshot(String source,
                                                         long fromSeqNo, long toSeqNo, boolean requiredFullRange) throws IOException;

    /**
     * Creates a new history snapshot for reading operations since {@code startingSeqNo} (inclusive).
     * The returned snapshot can be retrieved from either Lucene index or translog files.
     */
    public abstract Translog.Snapshot readHistoryOperations(String reason, HistorySource historySource,
                                                            long startingSeqNo) throws IOException;

    /**
     * Returns the estimated number of history operations whose seq# at least {@code startingSeqNo}(inclusive) in this engine.
     */
    public abstract int estimateNumberOfHistoryOperations(String reason, HistorySource historySource,
                                                          long startingSeqNo) throws IOException;

    /**
     * Checks if this engine has every operations since  {@code startingSeqNo}(inclusive) in its history (either Lucene or translog)
     */
    public abstract boolean hasCompleteOperationHistory(String reason, HistorySource historySource,
                                                        long startingSeqNo) throws IOException;

    /**
     * Gets the minimum retained sequence number for this engine.
     *
     * @return the minimum retained sequence number
     */
    public abstract long getMinRetainedSeqNo();

    public abstract TranslogStats getTranslogStats();

    /**
     * Returns the last location that the translog of this engine has written into.
     */
    public abstract Translog.Location getTranslogLastWriteLocation();

    protected final void ensureOpen(Exception suppressed) {
        if (isClosed.get()) {
            AlreadyClosedException ace = new AlreadyClosedException(shardId + " engine is closed", failedEngine.get());
            if (suppressed != null) {
                ace.addSuppressed(suppressed);
            }
            throw ace;
        }
    }

    protected final void ensureOpen() {
        ensureOpen(null);
    }

    /** get commits stats for the last commit */
    public final CommitStats commitStats() {
        return new CommitStats(getLastCommittedSegmentInfos());
    }

    /**
     * @return the persisted local checkpoint for this Engine
     */
    public abstract long getPersistedLocalCheckpoint();

    /**
     * @return a {@link SeqNoStats} object, using local state and the supplied global checkpoint
     */
    public abstract SeqNoStats getSeqNoStats(long globalCheckpoint);

    /**
     * Returns the latest global checkpoint value that has been persisted in the underlying storage (i.e. translog's checkpoint)
     */
    public abstract long getLastSyncedGlobalCheckpoint();

    /** How much heap is used that would be freed by a refresh.  Note that this may throw {@link AlreadyClosedException}. */
    public abstract long getIndexBufferRAMBytesUsed();

    final Segment[] getSegmentInfo(SegmentInfos lastCommittedSegmentInfos) {
        ensureOpen();
        Map<String, Segment> segments = new HashMap<>();
        // first, go over and compute the search ones...
        try (Searcher searcher = acquireSearcher("segments", SearcherScope.EXTERNAL)) {
            for (LeafReaderContext ctx : searcher.getIndexReader().getContext().leaves()) {
                fillSegmentInfo(Lucene.segmentReader(ctx.reader()), true, segments);
            }
        }

        try (Searcher searcher = acquireSearcher("segments", SearcherScope.INTERNAL)) {
            for (LeafReaderContext ctx : searcher.getIndexReader().getContext().leaves()) {
                SegmentReader segmentReader = Lucene.segmentReader(ctx.reader());
                if (segments.containsKey(segmentReader.getSegmentName()) == false) {
                    fillSegmentInfo(segmentReader, false, segments);
                }
            }
        }

        // now, correlate or add the committed ones...
        if (lastCommittedSegmentInfos != null) {
            for (SegmentCommitInfo info : lastCommittedSegmentInfos) {
                Segment segment = segments.get(info.info.name);
                if (segment == null) {
                    segment = new Segment(info.info.name);
                    segment.search = false;
                    segment.committed = true;
                    segment.delDocCount = info.getDelCount() + info.getSoftDelCount();
                    segment.docCount = info.info.maxDoc() - segment.delDocCount;
                    segment.version = info.info.getVersion();
                    segment.compound = info.info.getUseCompoundFile();
                    try {
                        segment.sizeInBytes = info.sizeInBytes();
                    } catch (IOException e) {
                        logger.trace(() -> new ParameterizedMessage("failed to get size for [{}]", info.info.name), e);
                    }
                    segment.segmentSort = info.info.getIndexSort();
                    segment.attributes = info.info.getAttributes();
                    segment.docsWithSource = -1;
                    segments.put(info.info.name, segment);
                } else {
                    segment.committed = true;
                }
            }
        }

        Segment[] segmentsArr = segments.values().toArray(new Segment[segments.values().size()]);
        Arrays.sort(segmentsArr, Comparator.comparingLong(Segment::getGeneration));
        return segmentsArr;
    }

    private void fillSegmentInfo(SegmentReader segmentReader, boolean search, Map<String, Segment> segments) {
        SegmentCommitInfo info = segmentReader.getSegmentInfo();
        assert segments.containsKey(info.info.name) == false;
        Segment segment = new Segment(info.info.name);
        segment.search = search;
        segment.docCount = segmentReader.numDocs();
        segment.delDocCount = segmentReader.numDeletedDocs();
        segment.version = info.info.getVersion();
        segment.compound = info.info.getUseCompoundFile();
        try {
            segment.sizeInBytes = info.sizeInBytes();
        } catch (IOException e) {
            logger.trace(() -> new ParameterizedMessage("failed to get size for [{}]", info.info.name), e);
        }
        segment.segmentSort = info.info.getIndexSort();
        segment.attributes = info.info.getAttributes();
        segment.docsWithSource = docsWithSource(segmentReader, info);
        // TODO: add more fine grained mem stats values to per segment info here
        segments.put(info.info.name, segment);
    }

    private int docsWithSource(SegmentReader reader, SegmentCommitInfo info) {
        if (this.store.indexSettings().getIndexVersionCreated().before(StoredRowLookup.PARTIAL_STORED_SOURCE_VERSION)) {
            return reader.maxDoc();
        }
        try {
            NumericDocValues dv = reader.getNumericDocValues(SysColumns.Source.RECOVERY_NAME);
            if (dv == null) {
                return 0;
            }
            return (int) dv.cost();
        } catch (IOException e) {
            logger.trace(() -> new ParameterizedMessage("failed to get source information for [{}]", info.info.name), e);
            return -1;
        }
    }

    /**
     * The list of segments in the engine.
     */
    public abstract List<Segment> segments();

    public boolean refreshNeeded() {
        if (store.tryIncRef()) {
            /*
              we need to inc the store here since we acquire a searcher and that might keep a file open on the
              store. this violates the assumption that all files are closed when
              the store is closed so we need to make sure we increment it here
             */
            try {
                try (Searcher searcher = acquireSearcher("refresh_needed", SearcherScope.EXTERNAL)) {
                    return searcher.getDirectoryReader().isCurrent() == false;
                }
            } catch (IOException e) {
                logger.error("failed to access searcher manager", e);
                failEngine("failed to access searcher manager", e);
                throw new EngineException(shardId, "failed to access searcher manager", e);
            } finally {
                store.decRef();
            }
        }
        return false;
    }

    /**
     * Synchronously refreshes the engine for new search operations to reflect the latest
     * changes.
     */
    public abstract void refresh(String source) throws EngineException;

    /**
     * Synchronously refreshes the engine for new search operations to reflect the latest
     * changes unless another thread is already refreshing the engine concurrently.
     *
     * @return <code>true</code> if the a refresh happened. Otherwise <code>false</code>
     */
    public abstract boolean maybeRefresh(String source) throws EngineException;

    /**
     * Called when our engine is using too much heap and should move buffered indexed/deleted documents to disk.
     */
    // NOTE: do NOT rename this to something containing flush or refresh!
    public abstract void writeIndexingBuffer() throws EngineException;

    /**
     * Checks if this engine should be flushed periodically.
     * This check is mainly based on the uncommitted translog size and the translog flush threshold setting.
     */
    public abstract boolean shouldPeriodicallyFlush();

    /**
     * Flushes the state of the engine including the transaction log, clearing memory.
     *
     * @param force         if <code>true</code> a lucene commit is executed even if no changes need to be committed.
     * @param waitIfOngoing if <code>true</code> this call will block until all currently running flushes have finished.
     *                      Otherwise this call will return without blocking.
     * @return the commit Id for the resulting commit
     */
    public abstract CommitId flush(boolean force, boolean waitIfOngoing) throws EngineException;

    /**
     * Flushes the state of the engine including the transaction log, clearing memory and persisting
     * documents in the lucene index to disk including a potentially heavy and durable fsync operation.
     * This operation is not going to block if another flush operation is currently running and won't write
     * a lucene commit if nothing needs to be committed.
     *
     * @return the commit Id for the resulting commit
     */
    public final CommitId flush() throws EngineException {
        return flush(false, false);
    }


    /**
     * checks and removes translog files that no longer need to be retained. See
     * {@link org.elasticsearch.index.translog.TranslogDeletionPolicy} for details
     */
    public abstract void trimUnreferencedTranslogFiles() throws EngineException;

    /**
     * Tests whether or not the translog generation should be rolled to a new generation.
     * This test is based on the size of the current generation compared to the configured generation threshold size.
     *
     * @return {@code true} if the current generation should be rolled to a new generation
     */
    public abstract boolean shouldRollTranslogGeneration();

    /**
     * Rolls the translog generation and cleans unneeded.
     */
    public abstract void rollTranslogGeneration() throws EngineException;

    /**
     * Triggers a forced merge on this engine
     */
    public abstract void forceMerge(boolean flush,
                                    int maxNumSegments,
                                    boolean onlyExpungeDeletes,
                                    @Nullable String forceMergeUUID) throws EngineException, IOException;

    /**
     * Snapshots the most recent index and returns a handle to it. If needed will try and "commit" the
     * lucene index to make sure we have a "fresh" copy of the files to snapshot.
     *
     * @param flushFirst indicates whether the engine should flush before returning the snapshot
     */
    public abstract IndexCommitRef acquireLastIndexCommit(boolean flushFirst) throws EngineException;

    /**
     * Snapshots the most recent safe index commit from the engine.
     */
    public abstract IndexCommitRef acquireSafeIndexCommit() throws EngineException;

    /**
     * @return a summary of the contents of the current safe commit
     */
    public abstract SafeCommitInfo getSafeCommitInfo();

    /**
     * If the specified throwable contains a fatal error in the throwable graph, such a fatal error will be thrown. Callers should ensure
     * that there are no catch statements that would catch an error in the stack as the fatal error here should go uncaught and be handled
     * by the uncaught exception handler that we install during bootstrap. If the specified throwable does indeed contain a fatal error, the
     * specified message will attempt to be logged before throwing the fatal error. If the specified throwable does not contain a fatal
     * error, this method is a no-op.
     *
     * @param maybeMessage the message to maybe log
     * @param maybeFatal   the throwable that maybe contains a fatal error
     */
    @SuppressWarnings("finally")
    private void maybeDie(final String maybeMessage, final Throwable maybeFatal) {
        Exceptions.maybeError(maybeFatal).ifPresent(error -> {
            try {
                logger.error(maybeMessage, error);
            } finally {
                throw error;
            }
        });
    }

    /**
     * fail engine due to some error. the engine will also be closed.
     * The underlying store is marked corrupted iff failure is caused by index corruption
     */
    public void failEngine(String reason, @Nullable Exception failure) {
        if (failure != null) {
            maybeDie(reason, failure);
        }
        if (failEngineLock.tryLock()) {
            try {
                if (failedEngine.get() != null) {
                    logger.warn(() -> new ParameterizedMessage("tried to fail engine but engine is already failed. ignoring. [{}]", reason), failure);
                    return;
                }
                // this must happen before we close IW or Translog such that we can check this state to opt out of failing the engine
                // again on any caught AlreadyClosedException
                failedEngine.set((failure != null) ? failure : new IllegalStateException(reason));
                try {
                    // we just go and close this engine - no way to recover
                    closeNoLock("engine failed on: [" + reason + "]", closedLatch);
                } finally {
                    logger.warn(() -> new ParameterizedMessage("failed engine [{}]", reason), failure);
                    // we must set a failure exception, generate one if not supplied
                    // we first mark the store as corrupted before we notify any listeners
                    // this must happen first otherwise we might try to reallocate so quickly
                    // on the same node that we don't see the corrupted marker file when
                    // the shard is initializing
                    if (Lucene.isCorruptionException(failure)) {
                        if (store.tryIncRef()) {
                            try {
                                store.markStoreCorrupted(new IOException("failed engine (reason: [" + reason + "])", SQLExceptions.unwrapCorruption(failure)));
                            } catch (IOException e) {
                                logger.warn("Couldn't mark store corrupted", e);
                            } finally {
                                store.decRef();
                            }
                        } else {
                            logger.warn(() ->
                                new ParameterizedMessage("tried to mark store as corrupted but store is already closed. [{}]", reason),
                                failure
                            );
                        }
                    }
                    eventListener.onFailedEngine(reason, failure);
                }
            } catch (Exception inner) {
                if (failure != null) inner.addSuppressed(failure);
                // don't bubble up these exceptions up
                logger.warn("failEngine threw exception", inner);
            }
        } else {
            logger.debug(() -> new ParameterizedMessage("tried to fail engine but could not acquire lock - engine should be failed by now [{}]", reason), failure);
        }
    }

    /** Check whether the engine should be failed */
    protected boolean maybeFailEngine(String source, Exception e) {
        if (Lucene.isCorruptionException(e)) {
            failEngine("corrupt file (source: [" + source + "])", e);
            return true;
        }
        return false;
    }


    public interface EventListener {
        /**
         * Called when a fatal exception occurred
         */
        default void onFailedEngine(String reason, @Nullable Exception e) {
        }
    }

    public static class Searcher extends IndexSearcher implements Releasable {

        private final String source;
        private final Closeable onClose;

        public Searcher(String source,
                        IndexReader reader,
                        QueryCache queryCache,
                        QueryCachingPolicy queryCachingPolicy,
                        Closeable onClose) {
            super(reader);
            setQueryCache(queryCache);
            setQueryCachingPolicy(queryCachingPolicy);
            this.source = source;
            this.onClose = onClose;
        }

        /**
         * The source that caused this searcher to be acquired.
         */
        public String source() {
            return source;
        }

        public DirectoryReader getDirectoryReader() {
            if (getIndexReader() instanceof DirectoryReader) {
                return (DirectoryReader) getIndexReader();
            }
            throw new IllegalStateException("Can't use " + getIndexReader().getClass() + " as a directory reader");
        }

        @Override
        public void close() {
            try {
                onClose.close();
            } catch (IOException e) {
                throw new UncheckedIOException("failed to close", e);
            } catch (AlreadyClosedException e) {
                // This means there's a bug somewhere: don't suppress it
                throw new AssertionError(e);
            }
        }
    }

    public abstract static class Operation {

        /** type of operation (index, delete), subclasses use static types */
        public enum TYPE {
            INDEX, DELETE, NO_OP;
        }

        private final Term uid;
        private final long version;
        private final long seqNo;
        private final long primaryTerm;
        private final VersionType versionType;
        private final Origin origin;
        private final long startTime;

        public Operation(Term uid, long seqNo, long primaryTerm, long version, VersionType versionType, Origin origin, long startTime) {
            this.uid = uid;
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
            this.version = version;
            this.versionType = versionType;
            this.origin = origin;
            this.startTime = startTime;
        }

        public enum Origin {
            PRIMARY,
            REPLICA,
            PEER_RECOVERY,
            LOCAL_TRANSLOG_RECOVERY,
            LOCAL_RESET;

            public boolean isRecovery() {
                return this == PEER_RECOVERY || this == LOCAL_TRANSLOG_RECOVERY;
            }

            boolean isFromTranslog() {
                return this == LOCAL_TRANSLOG_RECOVERY || this == LOCAL_RESET;
            }
        }

        public Origin origin() {
            return this.origin;
        }

        public Term uid() {
            return this.uid;
        }

        public long version() {
            return this.version;
        }

        public long seqNo() {
            return seqNo;
        }

        public long primaryTerm() {
            return primaryTerm;
        }

        public abstract int estimatedSizeInBytes();

        public VersionType versionType() {
            return this.versionType;
        }

        /**
         * Returns operation start time in nanoseconds.
         */
        public long startTime() {
            return this.startTime;
        }

        abstract String id();

        public abstract TYPE operationType();
    }

    public static class Index extends Operation {

        private final ParsedDocument doc;
        private final long autoGeneratedIdTimestamp;
        private final boolean isRetry;
        private final long ifSeqNo;
        private final long ifPrimaryTerm;

        public Index(Term uid,
                     ParsedDocument doc,
                     long seqNo,
                     long primaryTerm,
                     long version,
                     VersionType versionType,
                     Origin origin,
                     long startTime,
                     long autoGeneratedIdTimestamp,
                     boolean isRetry,
                     long ifSeqNo,
                     long ifPrimaryTerm) {
            super(uid, seqNo, primaryTerm, version, versionType, origin, startTime);
            assert (origin == Origin.PRIMARY) == (versionType != null) : "invalid version_type=" + versionType + " for origin=" + origin;
            assert ifPrimaryTerm >= 0 : "ifPrimaryTerm [" + ifPrimaryTerm + "] must be non negative";
            assert ifSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO || ifSeqNo >= 0 :
                "ifSeqNo [" + ifSeqNo + "] must be non negative or unset";
            assert (origin == Origin.PRIMARY) || (ifSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO && ifPrimaryTerm == 0) :
                "cas operations are only allowed if origin is primary. get [" + origin + "]";
            this.doc = doc;
            this.isRetry = isRetry;
            this.autoGeneratedIdTimestamp = autoGeneratedIdTimestamp;
            this.ifSeqNo = ifSeqNo;
            this.ifPrimaryTerm = ifPrimaryTerm;
        }


        @VisibleForTesting
        public Index(Term uid, long primaryTerm, ParsedDocument doc) {
            this(
                uid,
                doc,
                UNASSIGNED_SEQ_NO,
                primaryTerm,
                Versions.MATCH_ANY,
                VersionType.INTERNAL,
                Origin.PRIMARY,
                System.nanoTime(),
                -1,
                false,
                UNASSIGNED_SEQ_NO,
                0
            );
        }

        public ParsedDocument parsedDoc() {
            return this.doc;
        }

        @Override
        public String id() {
            return this.doc.id();
        }

        @Override
        public TYPE operationType() {
            return TYPE.INDEX;
        }

        public Document document() {
            return this.doc.doc();
        }

        public BytesReference source() {
            return this.doc.source();
        }

        @Override
        public int estimatedSizeInBytes() {
            return id().length() * 2 + source().length() + 12;
        }

        /**
         * Returns a positive timestamp if the ID of this document is auto-generated by elasticsearch.
         * if this property is non-negative indexing code might optimize the addition of this document
         * due to it's append only nature.
         */
        public long getAutoGeneratedIdTimestamp() {
            return autoGeneratedIdTimestamp;
        }

        /**
         * Returns <code>true</code> if this index requests has been retried on the coordinating node and can therefor be delivered
         * multiple times. Note: this might also be set to true if an equivalent event occurred like the replay of the transaction log
         */
        public boolean isRetry() {
            return isRetry;
        }

        public long getIfSeqNo() {
            return ifSeqNo;
        }

        public long getIfPrimaryTerm() {
            return ifPrimaryTerm;
        }
    }

    public static class Delete extends Operation {

        private final String id;
        private final long ifSeqNo;
        private final long ifPrimaryTerm;

        public Delete(String id,
                      Term uid,
                      long seqNo,
                      long primaryTerm,
                      long version,
                      VersionType versionType,
                      Origin origin,
                      long startTime,
                      long ifSeqNo,
                      long ifPrimaryTerm) {
            super(uid, seqNo, primaryTerm, version, versionType, origin, startTime);
            assert (origin == Origin.PRIMARY) == (versionType != null) : "invalid version_type=" + versionType + " for origin=" + origin;
            assert ifPrimaryTerm >= 0 : "ifPrimaryTerm [" + ifPrimaryTerm + "] must be non negative";
            assert ifSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO || ifSeqNo >= 0 :
                "ifSeqNo [" + ifSeqNo + "] must be non negative or unset";
            assert (origin == Origin.PRIMARY) || (ifSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO && ifPrimaryTerm == 0) :
                "cas operations are only allowed if origin is primary. get [" + origin + "]";
            this.id = Objects.requireNonNull(id);
            this.ifSeqNo = ifSeqNo;
            this.ifPrimaryTerm = ifPrimaryTerm;
        }

        @VisibleForTesting
        public Delete(String id, Term uid, long primaryTerm) {
            this(
                id,
                uid,
                UNASSIGNED_SEQ_NO,
                primaryTerm,
                Versions.MATCH_ANY,
                VersionType.INTERNAL,
                Origin.PRIMARY,
                System.nanoTime(),
                UNASSIGNED_SEQ_NO,
                0
            );
        }

        @Override
        public String id() {
            return this.id;
        }

        @Override
        public TYPE operationType() {
            return TYPE.DELETE;
        }

        @Override
        public int estimatedSizeInBytes() {
            return (uid().field().length() + uid().text().length()) * 2 + 20;
        }

        public long getIfSeqNo() {
            return ifSeqNo;
        }

        public long getIfPrimaryTerm() {
            return ifPrimaryTerm;
        }
    }

    public static class NoOp extends Operation {

        private final String reason;

        public String reason() {
            return reason;
        }

        public NoOp(final long seqNo, final long primaryTerm, final Origin origin, final long startTime, final String reason) {
            super(null, seqNo, primaryTerm, Versions.NOT_FOUND, null, origin, startTime);
            this.reason = reason;
        }

        @Override
        public Term uid() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long version() {
            throw new UnsupportedOperationException();
        }

        @Override
        public VersionType versionType() {
            throw new UnsupportedOperationException();
        }

        @Override
        String id() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TYPE operationType() {
            return TYPE.NO_OP;
        }

        @Override
        public int estimatedSizeInBytes() {
            return 2 * reason.length() + 2 * Long.BYTES;
        }

    }

    public static class Get {
        private final Term uid;
        private final String id;
        private long version = Versions.MATCH_ANY;
        private VersionType versionType = VersionType.INTERNAL;
        private long ifSeqNo = UNASSIGNED_SEQ_NO;
        private long ifPrimaryTerm = UNASSIGNED_PRIMARY_TERM;

        public Get(String id, Term uid) {
            this.id = id;
            this.uid = uid;
        }

        public String id() {
            return id;
        }

        public Term uid() {
            return uid;
        }

        public long version() {
            return version;
        }

        public Get version(long version) {
            this.version = version;
            return this;
        }

        public VersionType versionType() {
            return versionType;
        }

        public Get versionType(VersionType versionType) {
            this.versionType = versionType;
            return this;
        }

        public Get setIfSeqNo(long seqNo) {
            this.ifSeqNo = seqNo;
            return this;
        }

        public long getIfSeqNo() {
            return ifSeqNo;
        }

        public Get setIfPrimaryTerm(long primaryTerm) {
            this.ifPrimaryTerm = primaryTerm;
            return this;
        }

        public long getIfPrimaryTerm() {
            return ifPrimaryTerm;
        }

    }

    public static class GetResult implements Releasable {
        @Nullable
        private final DocIdAndVersion docIdAndVersion;
        private final Engine.Searcher searcher;

        public static final GetResult NOT_EXISTS = new GetResult(null, null);

        public GetResult(DocIdAndVersion docIdAndVersion, Engine.Searcher searcher) {
            this.docIdAndVersion = docIdAndVersion;
            this.searcher = searcher;
        }

        public Engine.Searcher searcher() {
            return this.searcher;
        }

        @Nullable
        public DocIdAndVersion docIdAndVersion() {
            return docIdAndVersion;
        }

        public boolean fromTranslog() {
            return docIdAndVersion != null && docIdAndVersion.reader instanceof TranslogLeafReader;
        }

        @Override
        public void close() {
            release();
        }

        public void release() {
            Releasables.close(searcher);
        }
    }

    /**
     * Method to close the engine while the write lock is held.
     * Must decrement the supplied when closing work is done and resources are
     * freed.
     */
    protected abstract void closeNoLock(String reason, CountDownLatch closedLatch);

    /**
     * Flush the engine (committing segments to disk and truncating the
     * translog) and close it.
     */
    public void flushAndClose() throws IOException {
        if (isClosed.get() == false) {
            logger.trace("flushAndClose now acquire writeLock");
            try (ReleasableLock lock = writeLock.acquire()) {
                logger.trace("flushAndClose now acquired writeLock");
                try {
                    logger.debug("flushing shard on close - this might take some time to sync files to disk");
                    try {
                        flush(); // TODO we might force a flush in the future since we have the write lock already even though recoveries are running.
                    } catch (AlreadyClosedException ex) {
                        logger.debug("engine already closed - skipping flushAndClose");
                    }
                } finally {
                    close(); // double close is not a problem
                }
            }
        }
        awaitPendingClose();
    }

    @Override
    public void close() throws IOException {
        if (isClosed.get() == false) { // don't acquire the write lock if we are already closed
            logger.debug("close now acquiring writeLock");
            try (ReleasableLock lock = writeLock.acquire()) {
                logger.debug("close acquired writeLock");
                closeNoLock("api", closedLatch);
            }
        }
        awaitPendingClose();
    }

    private void awaitPendingClose() {
        try {
            closedLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static class CommitId implements Writeable {

        private final byte[] id;

        public CommitId(byte[] id) {
            assert id != null;
            this.id = Arrays.copyOf(id, id.length);
        }

        /**
         * Read from a stream.
         */
        public CommitId(StreamInput in) throws IOException {
            assert in != null;
            this.id = in.readByteArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByteArray(id);
        }

        @Override
        public String toString() {
            return Base64.getEncoder().encodeToString(id);
        }

        public boolean idsEqual(byte[] id) {
            return Arrays.equals(id, this.id);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            CommitId commitId = (CommitId) o;

            return Arrays.equals(id, commitId.id);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(id);
        }
    }

    public static class IndexCommitRef implements Closeable {
        private final AtomicBoolean closed = new AtomicBoolean();
        private final CheckedRunnable<IOException> onClose;
        private final IndexCommit indexCommit;

        public IndexCommitRef(IndexCommit indexCommit, CheckedRunnable<IOException> onClose) {
            this.indexCommit = indexCommit;
            this.onClose = onClose;
        }

        @Override
        public void close() throws IOException {
            if (closed.compareAndSet(false, true)) {
                onClose.run();
            }
        }

        public IndexCommit getIndexCommit() {
            return indexCommit;
        }
    }

    public void onSettingsChanged(TimeValue translogRetentionAge, ByteSizeValue translogRetentionSize, long softDeletesRetentionOps) {

    }

    /**
     * Returns the timestamp of the last write in nanoseconds.
     * Note: this time might not be absolutely accurate since the {@link Operation#startTime()} is used which might be
     * slightly inaccurate.
     *
     * @see System#nanoTime()
     * @see Operation#startTime()
     */
    public long getLastWriteNanos() {
        return this.lastWriteNanos;
    }

    /**
     * Returns the real timestamp of the last write in milliseconds.
     */
    public long lastWriteTimestamp() {
        return this.startTimeMillis + ((lastWriteNanos - startTimeNanos) / 1_000_000);
    }


    /**
     * Request that this engine throttle incoming indexing requests to one thread.  Must be matched by a later call to {@link #deactivateThrottling()}.
     */
    public abstract void activateThrottling();

    /**
     * Reverses a previous {@link #activateThrottling} call.
     */
    public abstract void deactivateThrottling();

    /**
     * This method replays translog to restore the Lucene index which might be reverted previously.
     * This ensures that all acknowledged writes are restored correctly when this engine is promoted.
     *
     * @return the number of translog operations have been recovered
     */
    public abstract int restoreLocalHistoryFromTranslog(TranslogRecoveryRunner translogRecoveryRunner) throws IOException;

    /**
     * Fills up the local checkpoints history with no-ops until the local checkpoint
     * and the max seen sequence ID are identical.
     * @param primaryTerm the shards primary term this engine was created for
     * @return the number of no-ops added
     */
    public abstract int fillSeqNoGaps(long primaryTerm) throws IOException;

    /**
     * Performs recovery from the transaction log up to {@code recoverUpToSeqNo} (inclusive).
     * This operation will close the engine if the recovery fails.
     *
     * @param translogRecoveryRunner the translog recovery runner
     * @param recoverUpToSeqNo       the upper bound, inclusive, of sequence number to be recovered
     */
    public abstract Engine recoverFromTranslog(TranslogRecoveryRunner translogRecoveryRunner, long recoverUpToSeqNo) throws IOException;

    /**
     * Do not replay translog operations, but make the engine be ready.
     */
    public abstract void skipTranslogRecovery();

    /**
     * Tries to prune buffered deletes from the version map.
     */
    public abstract void maybePruneDeletes();

    /**
     * Returns the maximum auto_id_timestamp of all append-only index requests have been processed by this engine
     * or the auto_id_timestamp received from its primary shard via {@link #updateMaxUnsafeAutoIdTimestamp(long)}.
     * Notes this method returns the auto_id_timestamp of all append-only requests, not max_unsafe_auto_id_timestamp.
     */
    public long getMaxSeenAutoIdTimestamp() {
        return Translog.UNSET_AUTO_GENERATED_TIMESTAMP;
    }

    /**
     * Forces this engine to advance its max_unsafe_auto_id_timestamp marker to at least the given timestamp.
     * The engine will disable optimization for all append-only whose timestamp at most {@code newTimestamp}.
     */
    public abstract void updateMaxUnsafeAutoIdTimestamp(long newTimestamp);

    @FunctionalInterface
    public interface TranslogRecoveryRunner {
        int run(Engine engine, Translog.Snapshot snapshot) throws IOException;
    }

    /**
     * Returns the maximum sequence number of either update or delete operations have been processed in this engine
     * or the sequence number from {@link #advanceMaxSeqNoOfUpdatesOrDeletes(long)}. An index request is considered
     * as an update operation if it overwrites the existing documents in Lucene index with the same document id.
     * <p>
     * A note on the optimization using max_seq_no_of_updates_or_deletes:
     * For each operation O, the key invariants are:
     * <ol>
     *     <li> I1: There is no operation on docID(O) with seqno that is {@literal > MSU(O) and < seqno(O)} </li>
     *     <li> I2: If {@literal MSU(O) < seqno(O)} then docID(O) did not exist when O was applied; more precisely, if there is any O'
     *              with {@literal seqno(O') < seqno(O) and docID(O') = docID(O)} then the one with the greatest seqno is a delete.</li>
     * </ol>
     * <p>
     * When a receiving shard (either a replica or a follower) receives an operation O, it must first ensure its own MSU at least MSU(O),
     * and then compares its MSU to its local checkpoint (LCP). If {@literal LCP < MSU} then there's a gap: there may be some operations
     * that act on docID(O) about which we do not yet know, so we cannot perform an add. Note this also covers the case where a future
     * operation O' with {@literal seqNo(O') > seqNo(O) and docId(O') = docID(O)} is processed before O. In that case MSU(O') is at least
     * seqno(O') and this means {@literal MSU >= seqNo(O') > seqNo(O) > LCP} (because O wasn't processed yet).
     * <p>
     * However, if {@literal MSU <= LCP} then there is no gap: we have processed every {@literal operation <= LCP}, and no operation O'
     * with {@literal seqno(O') > LCP and seqno(O') < seqno(O) also has docID(O') = docID(O)}, because such an operation would have
     * {@literal seqno(O') > LCP >= MSU >= MSU(O)} which contradicts the first invariant. Furthermore in this case we immediately know
     * that docID(O) has been deleted (or never existed) without needing to check Lucene for the following reason. If there's no earlier
     * operation on docID(O) then this is clear, so suppose instead that the preceding operation on docID(O) is O':
     * 1. The first invariant above tells us that {@literal seqno(O') <= MSU(O) <= LCP} so we have already applied O' to Lucene.
     * 2. Also {@literal MSU(O) <= MSU <= LCP < seqno(O)} (we discard O if {@literal seqno(O) <= LCP}) so the second invariant applies,
     *    meaning that the O' was a delete.
     * <p>
     * Therefore, if {@literal MSU <= LCP < seqno(O)} we know that O can safely be optimized with and added to lucene with addDocument.
     * Moreover, operations that are optimized using the MSU optimization must not be processed twice as this will create duplicates
     * in Lucene. To avoid this we check the local checkpoint tracker to see if an operation was already processed.
     *
     * @see #advanceMaxSeqNoOfUpdatesOrDeletes(long)
     */
    public abstract long getMaxSeqNoOfUpdatesOrDeletes();

    /**
     * A replica shard receives a new max_seq_no_of_updates from its primary shard, then calls this method
     * to advance this marker to at least the given sequence number.
     */
    public abstract void advanceMaxSeqNoOfUpdatesOrDeletes(long maxSeqNoOfUpdatesOnPrimary);

    /**
     * Whether we should read history operations from translog or Lucene index
     */
    public enum HistorySource {
        TRANSLOG, INDEX
    }
}
