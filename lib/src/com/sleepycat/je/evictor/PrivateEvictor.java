/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2010 Oracle.  All rights reserved.
 *
 */

package com.sleepycat.je.evictor;

import java.util.Iterator;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.utilint.StatGroup;

/**
 * The standard Evictor that operates on the INList for a single environment.
 * A single iterator over the INList is used to implement getNextIN.
 */
public class PrivateEvictor extends Evictor {

    private EnvironmentImpl envImpl;

    private Iterator<IN> scanIter;

    public PrivateEvictor(EnvironmentImpl envImpl,
                          long wakeupInterval,
                          String name)
        throws DatabaseException {

        super(envImpl, wakeupInterval, name);
        this.envImpl = envImpl;
        scanIter = null;
    }

    @Override
    public StatGroup loadStats(StatsConfig config) {
        return super.loadStats(config);
    }

    @Override
    public void onWakeup()
        throws DatabaseException {

        if (!envImpl.isClosed()) {
            super.onWakeup();
        }
    }

    /**
     * Standard daemon method to set envImpl to null.
     */
    @Override
    public void clearEnv() {
        envImpl = null;
    }

    /**
     * Do nothing.
     */
    @Override
    public void noteINListChange(int nINs) {
    }

    /**
     * Only supported by SharedEvictor.
     */
    @Override
    public void addEnvironment(EnvironmentImpl unused) {
        throw EnvironmentFailureException.unexpectedState();
    }

    /**
     * Only supported by SharedEvictor.
     */
    @Override
    public void removeEnvironment(EnvironmentImpl unused) {
        throw EnvironmentFailureException.unexpectedState();
    }

    /**
     * Only supported by SharedEvictor.
     */
    @Override
    public boolean checkEnv(EnvironmentImpl env) {
        throw EnvironmentFailureException.unexpectedState();
    }

    /**
     * Initializes the iterator, and performs special eviction once per batch.
     */
    @Override
    long startBatch()
        throws DatabaseException {

        if (scanIter == null) {
            scanIter = envImpl.getInMemoryINs().iterator();
        }

        /* Perform special eviction without holding any latches. */
        return envImpl.specialEviction();
    }

    /**
     * Returns the simple INList size.
     */
    @Override
    int getMaxINsPerBatch() {
        return envImpl.getInMemoryINs().getSize();
    }

    /**
     * Returns the next IN, wrapping if necessary.
     */
    @Override
    IN getNextIN() {
        if (envImpl.getMemoryBudget().isTreeUsageAboveMinimum()) {
            if (!scanIter.hasNext()) {
                scanIter = envImpl.getInMemoryINs().iterator();
            }
            return scanIter.hasNext() ? scanIter.next() : null;
        } else {
            return null;
        }
    }

    /**
     * Return stats for the single INList covered by this evictor.
     */
    @Override
    StatGroup getINListStats(StatsConfig config) {
        return envImpl.getInMemoryINs().loadStats();
    }

    /* For unit testing only. */
    @Override
    Iterator<IN> getScanIterator() {
        return scanIter;
    }

    /* For unit testing only. */
    @Override
    void setScanIterator(Iterator<IN> iter) {
        scanIter = iter;
    }
}
