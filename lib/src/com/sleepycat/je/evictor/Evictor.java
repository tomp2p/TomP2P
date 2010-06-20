/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2010 Oracle.  All rights reserved.
 *
 */

package com.sleepycat.je.evictor;

import static com.sleepycat.je.evictor.EvictorStatDefinition.BIN_EVICTION_TYPE_DESC;
import static com.sleepycat.je.evictor.EvictorStatDefinition.BIN_FETCH;
import static com.sleepycat.je.evictor.EvictorStatDefinition.BIN_FETCH_MISS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_BINS_STRIPPED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_EVICT_PASSES;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_NODES_EVICTED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_NODES_SCANNED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_NODES_SELECTED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_REQUIRED_EVICT_BYTES;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_ROOT_NODES_EVICTED;
import static com.sleepycat.je.evictor.EvictorStatDefinition.EVICTOR_SHARED_CACHE_ENVS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.GROUP_DESC;
import static com.sleepycat.je.evictor.EvictorStatDefinition.GROUP_NAME;
import static com.sleepycat.je.evictor.EvictorStatDefinition.UPPER_IN_EVICTION_TYPE_DESC;
import static com.sleepycat.je.evictor.EvictorStatDefinition.UPPER_IN_FETCH;
import static com.sleepycat.je.evictor.EvictorStatDefinition.UPPER_IN_FETCH_MISS;
import static com.sleepycat.je.evictor.EvictorStatDefinition.LN_FETCH;
import static com.sleepycat.je.evictor.EvictorStatDefinition.LN_FETCH_MISS;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.cleaner.LocalUtilizationTracker;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.INList;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.latch.LatchSupport;
import com.sleepycat.je.recovery.Checkpointer;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.ChildReference;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.tree.SearchResult;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.tree.WithRootLatched;
import com.sleepycat.je.utilint.AtomicLongStat;
import com.sleepycat.je.utilint.DaemonThread;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.IntStat;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.StatDefinition;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;

/**
 * The Evictor looks through the INList for IN's and BIN's that are worthy of
 * eviction.  Once the nodes are selected, it removes all references to them so
 * that they can be GC'd by the JVM.
 */
public abstract class Evictor extends DaemonThread {
    
    /*
     * If new eviction source enums are added, a new stat is created, and 
     * EnvironmentStats must be updated to add a getter method.
     */
    public enum EvictionSource {
        /* Using ordinal for array values! */
        EVICTORTHREAD, MANUAL, CRITICAL, CACHEMODE;

        public StatDefinition getBINStatDef() {
            return new StatDefinition("nBINsEvicted" + toString(),
                                      BIN_EVICTION_TYPE_DESC);
        }

        public StatDefinition getUpperINStatDef() {
            return new StatDefinition("nUpperINsEvicted" + toString(),
                                      UPPER_IN_EVICTION_TYPE_DESC);
        }
    }

    private static final boolean DEBUG = false;

    private final MemoryBudget.Totals memBudgetTotals;

    /* Prevent endless eviction loops under extreme resource constraints. */
    private static final int MAX_BATCHES_PER_RUN = 100;

    /* true if eviction is happening. */
    private volatile boolean active;

    /* 1 node out of <nodesPerScan> are chosen for eviction. */
    private final int nodesPerScan;

    /* je.evictor.evictBytes */
    private final long evictBytesSetting;

    /* je.evictor.lruOnly */
    private final boolean evictByLruOnly;

    /* je.evictor.forceYield */
    private final boolean forcedYield;

    /* je.evictor.deadlockRetry */
    private final int deadlockRetries;

    /* for trace messages. */
    private NumberFormat formatter;

    /* Used to constrain the length of the eviction scan. */
    private long nNodesScannedThisRun;

    /*
     * Stats
     */
    private final StatGroup stats;
    /* The number of bytes we need to evict in order to get under budget. */
    private long currentRequiredEvictBytes = 0;
    private final LongStat requiredEvictBytes;
    /* Number of passes made to the evictor. */
    private final LongStat nEvictPasses;
    /* Number of nodes selected to evict. */
    private final LongStat nNodesSelected;
    /* Number of nodes scanned in order to select the eviction set */
    private final LongStat nNodesScanned;
    /* Whether isCacheFull ever returned true. */
    private boolean everFull;

    /*
     * Number of nodes evicted on this run. This could be understated, as a
     * whole subtree may have gone out with a single node.
     */
    private final LongStat nNodesEvicted;
    /* Number of closed database root nodes evicted on this run. */
    private final LongStat nRootNodesEvicted;
    /* Number of BINs stripped. */
    private final LongStat nBINsStripped;
    /* Number of envs sharing the cache. */
    protected IntStat sharedCacheEnvs;

    /*
     * Tree related cache hit/miss stats. A subset of the cache misses recorded
     * by the log manager, in that these only record tree node hits and misses.
     * Recorded by IN.fetchTarget, but grouped with evictor stats. Use
     * AtomicLongStat for multithreading safety.
     */
    private final AtomicLongStat nLNFetch;
    private final AtomicLongStat nBINFetch;
    private final AtomicLongStat nUpperINFetch;
    private final AtomicLongStat nLNFetchMiss;
    private final AtomicLongStat nBINFetchMiss;
    private final AtomicLongStat nUpperINFetchMiss;

    /*
     * Array of stats is indexed into by the EvictionSource ordinal value.
     * A EnumMap could have been an alternative, but would be heavier weight.
     */
    private final AtomicLongStat[] binEvictSources;
    private final AtomicLongStat[] inEvictSources;

    /* Debugging and unit test support. */
    EvictProfile evictProfile;
    private TestHook<Boolean> runnableHook;
    private TestHook<Object> preEvictINHook;

    Evictor(EnvironmentImpl envImpl, long wakeupInterval, String name)
        throws DatabaseException {

        super(wakeupInterval, name, envImpl);

        memBudgetTotals = envImpl.getMemoryBudget().getTotals();

        DbConfigManager configManager = envImpl.getConfigManager();
        nodesPerScan = configManager.getInt
            (EnvironmentParams.EVICTOR_NODES_PER_SCAN);
        evictBytesSetting = configManager.getLong
            (EnvironmentParams.EVICTOR_EVICT_BYTES);
        evictByLruOnly = configManager.getBoolean
            (EnvironmentParams.EVICTOR_LRU_ONLY);
        forcedYield = configManager.getBoolean
            (EnvironmentParams.EVICTOR_FORCED_YIELD);
        deadlockRetries = configManager.getInt
            (EnvironmentParams.EVICTOR_RETRY);

        evictProfile = new EvictProfile();

        active = false;

        /* Do the stats definitions. */
        stats = new StatGroup(GROUP_NAME, GROUP_DESC);
        requiredEvictBytes = new LongStat(stats, EVICTOR_REQUIRED_EVICT_BYTES);
        nEvictPasses = new LongStat(stats, EVICTOR_EVICT_PASSES);
        nNodesSelected = new LongStat(stats, EVICTOR_NODES_SELECTED);
        nNodesScanned = new LongStat(stats, EVICTOR_NODES_SCANNED);
        nNodesEvicted = new LongStat(stats, EVICTOR_NODES_EVICTED);
        nRootNodesEvicted = new LongStat(stats, EVICTOR_ROOT_NODES_EVICTED);
        nBINsStripped = new LongStat(stats, EVICTOR_BINS_STRIPPED);
        sharedCacheEnvs = new IntStat(stats, EVICTOR_SHARED_CACHE_ENVS);

        nLNFetch = new AtomicLongStat(stats, LN_FETCH);
        nBINFetch = new AtomicLongStat(stats, BIN_FETCH);
        nUpperINFetch = new AtomicLongStat(stats, UPPER_IN_FETCH);
        nLNFetchMiss = new AtomicLongStat(stats, LN_FETCH_MISS);
        nBINFetchMiss = new AtomicLongStat(stats, BIN_FETCH_MISS);
        nUpperINFetchMiss = new AtomicLongStat(stats, UPPER_IN_FETCH_MISS);

        EnumSet<EvictionSource> allSources = 
            EnumSet.allOf(EvictionSource.class);
            
        binEvictSources = new AtomicLongStat[allSources.size()];
        inEvictSources = new AtomicLongStat[allSources.size()];
        for (EvictionSource source : allSources) {
            binEvictSources[source.ordinal()] = 
                new AtomicLongStat(stats, source.getBINStatDef());
            inEvictSources[source.ordinal()] = 
                new AtomicLongStat(stats, source.getUpperINStatDef());
        }
    }

    abstract StatGroup getINListStats(StatsConfig config);

    /**
     * Load stats.
     */
    public StatGroup loadStats(StatsConfig config) {
        requiredEvictBytes.set(currentRequiredEvictBytes);

        StatGroup copy = stats.cloneGroup(config.getClear());
        copy.addAll(getINListStats(config));
        return copy;
    }

    /**
     * Return the number of retries when a deadlock exception occurs.
     */
    @Override
    protected long nDeadlockRetries() {
        return deadlockRetries;
    }

    /**
     * Wakeup the evictor only if it's not already active.
     */
    public void alert() {
        if (!active) {
            wakeup();
        }
    }

    /**
     * Called whenever the daemon thread wakes up from a sleep.
     */
    @Override
    public void onWakeup()
        throws DatabaseException {

        doEvict(EvictionSource.EVICTORTHREAD,
                false, // criticalEviction
                true); // backgroundIO
    }

    /**
     * May be called by the evictor thread on wakeup or programatically.
     */
    public void doEvict(EvictionSource source)
        throws DatabaseException {

        doEvict(source,
                false, // criticalEviction
                true); // backgroundIO
    }

    /**
     * Allows performing eviction during shutdown, which is needed when
     * during checkpointing and cleaner log file deletion.
     */
    private synchronized void doEvict(EvictionSource source,
                                      boolean criticalEviction,
                                      boolean backgroundIO)
        throws DatabaseException {

        /*
         * We use an active flag to prevent reentrant calls.  This is simpler
         * than ensuring that no reentrant eviction can occur in any caller.
         * We also use the active flag to determine when it is unnecessary to
         * wake up the evictor thread.
         */
        if (active) {
            return;
        }
        active = true;
        try {

            /*
             * Repeat as necessary to keep up with allocations.  Stop if no
             * progress is made, to prevent an infinite loop.
             */
            boolean progress = true;
            int nBatches = 0;
            while (progress &&
                   (nBatches < MAX_BATCHES_PER_RUN) &&
                   (criticalEviction || !isShutdownRequested()) &&
                   isRunnable(source)) {
                if (evictBatch
                    (source, backgroundIO, currentRequiredEvictBytes) == 0) {
                    progress = false;
                }
                nBatches += 1;
            }
        } finally {
            active = false;
        }
    }

    /**
     * Do a check on whether synchronous eviction is needed.
     *
     * Note that this method is intentionally not synchronized in order to
     * minimize overhead when checking for critical eviction.  This method is
     * called from application threads for every cursor operation.
     */
    public void doCriticalEviction(boolean backgroundIO) {

        final long currentUsage  = memBudgetTotals.getCacheUsage();
        final long maxMem = memBudgetTotals.getMaxMemory();
        final long over = currentUsage - maxMem;

        if (over > memBudgetTotals.getCriticalThreshold()) {
            if (DEBUG) {
                System.out.println("***critical detected: " + over);
            }
            doEvict(EvictionSource.CRITICAL,
                    true, // criticalEviction
                    backgroundIO);
        }

        if (forcedYield) {
            Thread.yield();
        }
    }

    /**
     * Each iteration will attempt to evict requiredEvictBytes, but will give
     * up after a complete pass over the INList.
     *
     * @return the number of bytes evicted, or zero if no progress was made.
     */
    long evictBatch(EvictionSource source,
                    boolean backgroundIO,
                    long reqEvictBytes)
        throws DatabaseException {

        nNodesScannedThisRun = 0;
        nEvictPasses.increment();

        assert evictProfile.clear(); // intentional side effect
        int nBatchSets = 0;

        /* Perform class-specific per-batch processing. */
        long evictBytes = startBatch();

        /* Must call getMaxINsPerBatch after startBatch. */
        int maxINsPerBatch = getMaxINsPerBatch();
        if (maxINsPerBatch == 0) {
            return evictBytes; // The INList(s) are empty.
        }

        try {

            /*
             * Keep evicting until we've freed enough memory or we've visited
             * the maximum number of nodes allowed. Each iteration of the while
             * loop is called an eviction batch.
             *
             * In order to prevent endless evicting, limit this run to one pass
             * over the IN list(s).
             */
            while ((evictBytes < reqEvictBytes) &&
                   (nNodesScannedThisRun <= maxINsPerBatch)) {

                IN target = selectIN(maxINsPerBatch);

                if (target == null) {
                    break;
                } else {
                    assert evictProfile.count(target);//intentional side effect

                    /*
                     * Check to make sure the DB was not deleted after
                     * selecting it, and prevent the DB from being deleted
                     * while we're working with it.
                     */
                    DatabaseImpl targetDb = target.getDatabase();
                    DbTree dbTree = targetDb.getDbEnvironment().getDbTree();
                    DatabaseImpl refreshedDb = null;
                    try {
                        refreshedDb = dbTree.getDb(targetDb.getId());
                        if (refreshedDb != null && !refreshedDb.isDeleted()) {
                            if (target.isDbRoot()) {
                                evictBytes += evictRoot(target, backgroundIO);
                            } else {
                                evictBytes += evictIN(target, backgroundIO,
                                                      source);
                            }
                        } else {

                            /*
                             * We don't expect to see an IN that is resident on
                             * the INList with a database that has finished
                             * delete processing, because it should have been
                             * removed from the INList during post-delete
                             * cleanup.  It may have been returned by the
                             * INList iterator after being removed from the
                             * INList (because we're using ConcurrentHashMap),
                             * but then IN.getInListResident should return
                             * false.
                             */
                            if (targetDb.isDeleteFinished() &&
                                target.getInListResident()) {
                                String inInfo =
                                    " IN type=" + target.getLogType() +
                                    " id=" + target.getNodeId() +
                                    " not expected on INList";
                                String errMsg = (refreshedDb == null) ?
                                    inInfo :
                                    ("Database " + refreshedDb.getDebugName() +
                                     " id=" + refreshedDb.getId() +
                                     " rootLsn=" +
                                     DbLsn.getNoFormatString
                                         (refreshedDb.getTree().getRootLsn()) +
                                     ' ' + inInfo);
                                throw EnvironmentFailureException.
                                    unexpectedState(errMsg);
                            }
                        }
                    } finally {
                        dbTree.releaseDb(refreshedDb);
                    }
                }
                nBatchSets++;
            }
        } finally {
            nNodesScanned.add(nNodesScannedThisRun);
        }

        assert LatchSupport.countLatchesHeld() == 0: "latches held = " +
            LatchSupport.countLatchesHeld();

        return evictBytes;
    }

    /**
     * Returns true if the JE cache level is above the point where it is likely
     * that the cache has filled, and is staying full.  This is not guaranteed,
     * since the level does not stay at a constant value.  But it is a good
     * enough indication to drive activities such as cache mode determination.
     * This method errs on the side of returning true sooner than the point
     * where the cache is actually full, as described below.
     */
    public boolean isCacheFull() {

        /*
         * When eviction occurs, normally the cache level goes down to roughly
         * MaxMemory minus evictBytesSetting.  However, because this is only an
         * approximation, we double the evictBytesSetting as a fudge factor.
         *
         * The idea is to return false from this method only when we're
         * relatively sure that the cache has not yet filled.  This will
         * prevent the return value from alternating between true and false
         * repeatedly as the result of normal eviction.
         */
        boolean ret = memBudgetTotals.getCacheUsage() +
                (2 * evictBytesSetting) >=
                memBudgetTotals.getMaxMemory();
        if (ret) {
            everFull = true;
        }
        return ret;
    }

    /**
     * Returns whether eviction has ever occurred, i.e., whether the cache has
     * ever filled.
     */
    public boolean wasCacheEverFull() {
        return currentRequiredEvictBytes > 0;
    }

    /**
     * Return true if eviction should happen.  As a side effect, if true is
     * returned the currentRequiredEvictBytes is set.
     */
    private boolean isRunnable(EvictionSource source) {
        long currentUsage  = memBudgetTotals.getCacheUsage();
        long maxMem = memBudgetTotals.getMaxMemory();
        long overBudget = currentUsage - maxMem;
        boolean doRun = (overBudget > 0);

        /* If running, figure out how much to evict. */
        if (doRun) {
            currentRequiredEvictBytes = overBudget + evictBytesSetting;
            /* Don't evict more than 50% of the cache. */
            if (currentUsage - currentRequiredEvictBytes < maxMem / 2) {
                currentRequiredEvictBytes = overBudget + (maxMem / 2);
            }
            if (DEBUG) {
                if (source == EvictionSource.CRITICAL) {
                    System.out.println("executed: critical runnable");
                }
            }
        }

        /* Unit testing, force eviction. */
        if (runnableHook != null) {
            doRun = runnableHook.getHookValue();
            currentRequiredEvictBytes = maxMem;
        }

        /*
         * This trace message is expensive, only generate if tracing at this
         * level is enabled.
         */
        if (logger != null && logger.isLoggable(Level.FINE)) {
            maybeInitFormatter();

            /*
             * Generate debugging output. Note that Runtime.freeMemory
             * fluctuates over time as the JVM grabs more memory, so you really
             * have to do totalMemory - freeMemory to get stack usage.  (You
             * can't get the concept of memory available from free memory.)
             */
            Runtime r = Runtime.getRuntime();
            long totalBytes = r.totalMemory();
            long freeBytes= r.freeMemory();
            long usedBytes = r.totalMemory() - r.freeMemory();
            StringBuffer sb = new StringBuffer();
            sb.append(" source=").append(source);
            sb.append(" doRun=").append(doRun);
            sb.append(" JEusedBytes=").append(formatter.format(currentUsage));
            sb.append(" requiredEvict=").
                append(formatter.format(currentRequiredEvictBytes));
            sb.append(" JVMtotalBytes= ").append(formatter.format(totalBytes));
            sb.append(" JVMfreeBytes= ").append(formatter.format(freeBytes));
            sb.append(" JVMusedBytes= ").append(formatter.format(usedBytes));
            LoggerUtils.logMsg
                (logger, envImpl, Level.FINE, sb.toString());
        }

        return doRun;
    }

    /**
     * Select a single node to evict.
     */
    private IN selectIN(int maxNodesToIterate) {
        /* Find the best target in the next <nodesPerScan> nodes. */
        IN target = null;
        long targetGeneration = Long.MAX_VALUE;
        int targetLevel = Integer.MAX_VALUE;
        boolean targetDirty = true;

        /* The nodesPerScan limit is on nodes that qualify for eviction. */
        int nCandidates = 0;

        /* The limit on iterated nodes is to prevent an infinite loop. */
        int nIterated = 0;

        while (nIterated <  maxNodesToIterate && nCandidates < nodesPerScan) {
            IN in = getNextIN();
            if (in == null) {
                break; // INList is empty
            }
            nIterated++;
            nNodesScannedThisRun++;

            DatabaseImpl db = in.getDatabase();

            /*
             * Ignore the IN if its database is deleted.  We have not called
             * getDb, so we can't guarantee that the DB is valid; get Db is
             * called and this is checked again after an IN is selected for
             * eviction.
             */
            if (db == null || db.isDeleted()) {
                continue;
            }

            /*
             * If this is a read-only environment, skip any dirty INs (recovery
             * dirties INs even in a read-only environment).
             */
            if (db.getDbEnvironment().isReadOnly() &&
                in.getDirty()) {
                continue;
            }

            /*
             * Only scan evictable or strippable INs.  This prevents higher
             * level INs from being selected for eviction, unless they are
             * part of an unused tree.
             */
            int evictType = in.getEvictionType();
            if (evictType == IN.MAY_NOT_EVICT) {
                continue;
            }

            /*
             * This node is in the scanned node set.  Select according to
             * the configured eviction policy.
             */
            if (evictByLruOnly) {

                /*
                 * Select the node with the lowest generation number,
                 * irrespective of tree level or dirtyness.
                 */
                if (targetGeneration > in.getGeneration()) {
                    targetGeneration = in.getGeneration();
                    target = in;
                }
            } else {

                /*
                 * Select first by tree level, then by dirtyness, then by
                 * generation/LRU.
                 */
                int level = normalizeLevel(in, evictType);
                if (targetLevel != level) {
                    if (targetLevel > level) {
                        targetLevel = level;
                        targetDirty = in.getDirty();
                        targetGeneration = in.getGeneration();
                        target = in;
                    }
                } else if (targetDirty != in.getDirty()) {
                    if (targetDirty) {
                        targetDirty = false;
                        targetGeneration = in.getGeneration();
                        target = in;
                    }
                } else {
                    if (targetGeneration > in.getGeneration()) {
                        targetGeneration = in.getGeneration();
                        target = in;
                    }
                }
            }
            nCandidates++;
        }

        if (target != null) {
            nNodesSelected.increment();
        }
        return target;
    }

    /**
     * Normalize the tree level of the given IN.
     *
     * Is public for unit testing.
     *
     * A BIN containing evictable LNs is given level 0, so it will be stripped
     * first.  For non-duplicate and DBMAP trees, the high order bits are
     * cleared to make their levels correspond; that way, all bottom level
     * nodes (BINs and DBINs) are given the same eviction priority.
     *
     * Note that BINs in a duplicate tree are assigned the same level as BINs
     * in a non-duplicate tree.  This isn't always optimimal, but is the best
     * we can do considering that BINs in duplicate trees may contain a mix of
     * LNs and DINs.
     *
     * BINs in the mapping tree are also assigned the same level as user DB
     * BINs.  When doing by-level eviction (lruOnly=false), this seems
     * counter-intuitive since we should evict user DB nodes before mapping DB
     * nodes.  But that does occur because mapping DB INs referencing an open
     * DB are unevictable.  The level is only used for selecting among
     * evictable nodes.
     *
     * If we did NOT normalize the level for the mapping DB, then INs for
     * closed evictable DBs would not be evicted until after all nodes in all
     * user DBs were evicted.  If there were large numbers of closed DBs, this
     * would have a negative performance impact.
     */
    public int normalizeLevel(IN in, int evictType) {

        int level = in.getLevel() & IN.LEVEL_MASK;

        if (level == 1 && evictType == IN.MAY_EVICT_LNS) {
            level = 0;
        }

        return level;
    }

    /**
     * Evict this DB root node.  [#13415]
     * @return number of bytes evicted.
     */
    private long evictRoot(final IN target,
                           final boolean backgroundIO)
        throws DatabaseException {

        final DatabaseImpl db = target.getDatabase();
        /* SharedEvictor uses multiple envs, do not use superclass envImpl. */ 
        final EnvironmentImpl envImpl = db.getDbEnvironment();
        final INList inList = envImpl.getInMemoryINs();

        class RootEvictor implements WithRootLatched {

            boolean flushed = false;
            long evictBytes = 0;

            public IN doWork(ChildReference root)
                throws DatabaseException {

                IN rootIN = (IN) root.fetchTarget(db, null);
                rootIN.latch(CacheMode.UNCHANGED);
                try {
                    /* Re-check that all conditions still hold. */
                    boolean isDirty = rootIN.getDirty();
                    if (rootIN == target &&
                        rootIN.isDbRoot() &&
                        rootIN.isEvictable() &&
                        !(envImpl.isReadOnly() && isDirty)) {

                        /* Flush if dirty. */
                        if (isDirty) {
                            long newLsn = rootIN.log
                                (envImpl.getLogManager(),
                                 false, // allowDeltas
                                 isProvisionalRequired(rootIN),
                                 backgroundIO,
                                 null); // parent
                            root.setLsn(newLsn);
                            flushed = true;
                        }

                        /* Take off the INList and adjust memory budget. */
                        inList.remove(rootIN);
                        evictBytes = rootIN.getBudgetedMemorySize();

                        /* Evict IN. */
                        root.clearTarget();

                        /* Stats */
                        nRootNodesEvicted.increment();
                    }
                } finally {
                    rootIN.releaseLatch();
                }
                return null;
            }
        }

        /* Attempt to evict the DB root IN. */
        RootEvictor evictor = new RootEvictor();
        db.getTree().withRootLatchedExclusive(evictor);

        /* If the root IN was flushed, write the dirtied MapLN. */
        if (evictor.flushed) {
            envImpl.getDbTree().modifyDbRoot(db);
        }

        return evictor.evictBytes;
    }

    /**
     * Strip or evict this node.
     *
     * @param source is EvictSource.CRITICAL or EVICTORTHREAD when this
     * operation is invoked by the evictor (either critical eviction or the
     * evictor background thread), and is EvictSource.CACHEMODE if invoked by a
     * user operation using CacheMode.EVICT_BIN.  If CACHEMODE, we will perform
     * the eviction regardless of whether:
     *  1) we have to wait for a latch, or
     *  2) the IN generation changes, or
     *  3) we are able to strip LNs.
     *
     * If not CACHEMODE, any of the above conditions will prevent eviction.
     *
     * @return number of bytes evicted.
     */
    public long evictIN(IN target, boolean backgroundIO, EvictionSource source)
        throws DatabaseException {

        DatabaseImpl db = target.getDatabase();
        /* SharedEvictor uses multiple envs, do not use superclass envImpl. */ 
        EnvironmentImpl envImpl = db.getDbEnvironment();
        long evictedBytes = 0;

        /*
         * Use a tracker to count lazily compressed, deferred write, LNs as
         * obsolete.  A local tracker is used to accumulate tracked obsolete
         * info so it can be added in a single call under the log write latch.
         * [#15365]
         */
        LocalUtilizationTracker localTracker = null;

        /*
         * Non-BIN INs are evicted by detaching them from their parent.  For
         * BINS, the first step is to remove deleted entries by compressing
         * the BIN. The evictor indicates that we shouldn't fault in
         * non-resident children during compression. After compression,
         * LN logging and LN stripping may be performed.
         *
         * If LN stripping is used, first we strip the BIN by logging any dirty
         * LN children and detaching all its resident LN targets.  If we make
         * progress doing that, we stop and will not evict the BIN itself until
         * possibly later.  If it has no resident LNs then we evict the BIN
         * itself using the "regular" detach-from-parent routine.
         *
         * If the cleaner is doing clustering, we don't do BIN stripping if we
         * can write out the BIN.  Specifically LN stripping is not performed
         * if the BIN is dirty AND the BIN is evictable AND cleaner
         * clustering is enabled.  In this case the BIN is going to be written
         * out soon, and with clustering we want to be sure to write out the
         * LNs with the BIN; therefore we don't do stripping.
         */

        /*
         * Use latchNoWait because if it's latched we don't want the cleaner
         * to hold up eviction while it migrates an entire BIN.  Latched INs
         * have a high generation value, so not evicting makes sense.  Pass
         * false because we don't want to change the generation during the
         * eviction process.
         */
        boolean inline = (source == EvictionSource.CACHEMODE);
        if (inline) {
            target.latch(CacheMode.UNCHANGED);
        } else {
            if (!target.latchNoWait(CacheMode.UNCHANGED)) {
                return evictedBytes;
            }
        }
        boolean targetIsLatched = true;
        try {
            if (target instanceof BIN) {
                /* First attempt to compress deleted, resident children. */
                localTracker = new LocalUtilizationTracker(envImpl);
                envImpl.lazyCompress(target, localTracker);

                /*
                 * Strip any resident LN targets right now. This may dirty
                 * the BIN if dirty LNs were written out. Note that
                 * migrated BIN entries cannot be stripped.
                 */
                evictedBytes = ((BIN) target).evictLNs();
                if (evictedBytes > 0) {
                    nBINsStripped.increment();
                }
            }

            /*
             * If we were able to free any memory by LN stripping above,
             * then we postpone eviction of the BIN until a later pass.
             * The presence of migrated entries would have inhibited LN
             * stripping. In that case, the BIN can still be evicted,
             * but the marked entries will have to be migrated. That would
             * happen when the target is logged in evictIN.
             */
            if (!inline && evictedBytes != 0) {
                return evictedBytes;
            }
            if (!target.isEvictable()) {
                return evictedBytes;
            }
            /* Regular eviction. */
            Tree tree = db.getTree();

            /*
             * Unit testing.  The target is latched and we are about to
             * release that latch and search for the parent.  Make sure
             * that other operations, such as dirtying an LN in the
             * target BIN, can occur safely in this window.  [#18227]
             */
            assert TestHookExecute.doHookIfSet(preEvictINHook);

            /* getParentINForChildIN unlatches target. */
            targetIsLatched = false;
            SearchResult result =
                tree.getParentINForChildIN
                (target,
                 true,   // requireExactMatch
                 CacheMode.UNCHANGED);

            if (result.exactParentFound) {
                evictedBytes = evictIN(target, result.parent,
                                       result.index, backgroundIO, source);
            }
        } finally {
            if (targetIsLatched) {
                target.releaseLatch();
            }
        }

        /*
         * Count obsolete nodes and write out modified file summaries for
         * recovery.  All latches must have been released. [#15365]
         */
        if (localTracker != null) {
            envImpl.getUtilizationProfile().flushLocalTracker(localTracker);
        }

        return evictedBytes;
    }

    /**
     * Evict an IN. Dirty nodes are logged before they're evicted.
     */
    private long evictIN(IN child,
                         IN parent,
                         int index,
                         boolean backgroundIO,
                         EvictionSource source)
        throws DatabaseException {

        long evictBytes = 0;
        try {
            assert parent.isLatchOwnerForWrite();

            long oldGenerationCount = child.getGeneration();

            /*
             * Get a new reference to the child, in case the reference
             * saved in the selection list became out of date because of
             * changes to that parent.
             */
            IN renewedChild = (IN) parent.getTarget(index);

            if (renewedChild == null) {
                return evictBytes;
            }
            
            boolean inline = (source == EvictionSource.CACHEMODE);
            if (!inline && renewedChild.getGeneration() > oldGenerationCount) {
                return evictBytes;
            }

            /*
             * See the evictIN() method in this class for an explanation for
             * calling latchNoWait().
             */
            if (inline) {
                renewedChild.latch(CacheMode.UNCHANGED);
            } else {
                if (!renewedChild.latchNoWait(CacheMode.UNCHANGED)) {
                    return evictBytes;
                }
            }
            try {
                if (!renewedChild.isEvictable()) {
                    return evictBytes;
                }

                DatabaseImpl db = renewedChild.getDatabase();
                /* Do not use superclass envImpl. */ 
                EnvironmentImpl envImpl = db.getDbEnvironment();

                /*
                 * Log the child if dirty and env is not r/o. Remove
                 * from IN list.
                 */
                long renewedChildLsn = DbLsn.NULL_LSN;
                boolean newChildLsn = false;
                if (renewedChild.getDirty()) {
                    if (!envImpl.isReadOnly()) {
                        boolean logProvisional =
                            isProvisionalRequired(renewedChild);

                        /*
                         * Log a full version (no deltas) and with
                         * cleaner migration allowed.
                         */
                        renewedChildLsn = renewedChild.log
                            (envImpl.getLogManager(),
                             false, // allowDeltas
                             logProvisional,
                             backgroundIO,
                             parent);
                        newChildLsn = true;
                    }
                } else {
                    renewedChildLsn = parent.getLsn(index);
                }

                if (renewedChildLsn != DbLsn.NULL_LSN) {
                    /* Take this off the inlist. */
                    envImpl.getInMemoryINs().remove(renewedChild);

                    evictBytes = renewedChild.getBudgetedMemorySize();
                    if (newChildLsn) {

                        /*
                         * Update the parent so its reference is
                         * null and it has the proper LSN.
                         */
                        parent.updateNode
                            (index, null /*node*/, renewedChildLsn,
                             null /*lnSlotKey*/);
                    } else {

                        /*
                         * Null out the reference, but don't dirty
                         * the node since only the reference
                         * changed.
                         */
                        parent.updateNode
                            (index, (Node) null /*node*/,
                             null /*lnSlotKey*/);
                    }

                    /* Stats */
                    nNodesEvicted.increment();
                    renewedChild.incEvictStats(source);
                }
            } finally {
                renewedChild.releaseLatch();
            }
        } finally {
            parent.releaseLatch();
        }

        return evictBytes;
    }

    public void incBINEvictStats(EvictionSource source) {
        binEvictSources[source.ordinal()].increment();
    }

    public void incINEvictStats(EvictionSource source) {
        inEvictSources[source.ordinal()].increment();
    }

    /**
     * Update the appropriate fetch stat, based on node type.
     */
    public void incLNFetchStats(boolean isMiss) {
        nLNFetch.increment();
        if (isMiss) {
            nLNFetchMiss.increment();
        }
    }

    public void incBINFetchStats(boolean isMiss) {
        nBINFetch.increment();
        if (isMiss) {
            nBINFetchMiss.increment();
        }
    }

    public void incINFetchStats(boolean isMiss) {
        nUpperINFetch.increment();
        if (isMiss) {
            nUpperINFetchMiss.increment();
        }
    }

    /*
     * @return true if the node must be logged provisionally.
     */
    private boolean isProvisionalRequired(IN target) {

        DatabaseImpl db = target.getDatabase();
        /* SharedEvictor uses multiple envs, do not use superclass envImpl. */ 
        EnvironmentImpl envImpl = db.getDbEnvironment();

        /*
         * The evictor has to log provisionally in two cases:
         * a - the checkpointer is in progress, and is at a level above the
         * target eviction victim. We don't want the evictor's actions to
         * introduce an IN that has not cascaded up properly.
         * b - the eviction target is part of a deferred write database.
         */
        if (db.isDeferredWriteMode()) {
            return true;
        }

        /*
         * The checkpointer could be null if it was shutdown or never
         * started.
         */
        Checkpointer ckpter = envImpl.getCheckpointer();
        if ((ckpter != null) &&
            (target.getLevel() < ckpter.getHighestFlushLevel(db))) {
            return true;
        }

        return false;
    }

    private void maybeInitFormatter() {
        if (formatter == null) {
            formatter = NumberFormat.getNumberInstance();
        }
    }

    /* For unit testing only. */
    public void setRunnableHook(TestHook<Boolean> hook) {
        runnableHook = hook;
    }

    /* For unit testing only. */
    public void setPreEvictINHook(TestHook<Object> hook) {
        preEvictINHook = hook;
    }

    /**
     * Standard daemon method to set envImpl to null.
     */
    public abstract void clearEnv();

    /**
     * Called whenever INs are added to, or removed from, the INList.
     */
    public abstract void noteINListChange(int nINs);

    /**
     * Only supported by SharedEvictor.
     */
    public abstract void addEnvironment(EnvironmentImpl envImpl);

    /**
     * Only supported by SharedEvictor.
     */
    public abstract void removeEnvironment(EnvironmentImpl envImpl);

    /**
     * Only supported by SharedEvictor.
     */
    public abstract boolean checkEnv(EnvironmentImpl env);

    /**
     * Perform class-specific batch processing: Initialize iterator, perform
     * UtilizationTracker eviction, etc.  No latches may be held when this
     * method is called.
     *
     * startBatch must be called before getMaxINsPerBatch.
     */
    abstract long startBatch()
        throws DatabaseException;

    /**
     * Returns the approximate number of total INs in the INList(s).  One
     * eviction batch will scan at most this number of INs.  If zero is
     * returned, selectIN will not be called.
     *
     * startBatch must be called before getMaxINsPerBatch.
     */
    abstract int getMaxINsPerBatch();

    /**
     * Returns the next IN in the INList(s), wrapping if necessary.
     */
    abstract IN getNextIN();

    /* For unit testing only.  Supported only by PrivateEvictor. */
    abstract Iterator<IN> getScanIterator();

    /* For unit testing only.  Supported only by PrivateEvictor. */
    abstract void setScanIterator(Iterator<IN> iter);

    /* For debugging and unit tests. */
    static class EvictProfile {
        /* Keep a list of candidate nodes. */
        private final List<Long> candidates = new ArrayList<Long>();

        /* Remember that this node was targeted. */
        public boolean count(IN target) {
            candidates.add(Long.valueOf(target.getNodeId()));
            return true;
        }

        public List<Long> getCandidates() {
            return candidates;
        }

        public boolean clear() {
            candidates.clear();
            return true;
        }
    }
}
