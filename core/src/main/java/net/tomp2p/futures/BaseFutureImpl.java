/*
 * Copyright 2009 Thomas Bocek
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package net.tomp2p.futures;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import net.tomp2p.connection.ConnectionBean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The base for all BaseFuture implementations. Be aware of possible deadlocks. Never await from a listener. This class
 * is heavily inspired by MINA and Netty.
 * 
 * @param <K>
 *            The class that extends BaseFuture and is used to return back the type for method calls. E.g, if K is
 *            FutureDHT await() returns FutureDHT.
 * @author Thomas Bocek
 */
public abstract class BaseFutureImpl<K extends BaseFuture> implements BaseFuture {
    private static final Logger LOG = LoggerFactory.getLogger(BaseFutureImpl.class);

    // Listeners that gets notified if the future finished
    private final List<BaseFutureListener<? extends BaseFuture>> listeners = 
            new ArrayList<BaseFutureListener<? extends BaseFuture>>(1);

    // While a future is running, the process may add cancellations for faster
    // cancel operations, e.g. cancel connection attempt
    private final List<Cancel> cancels = new ArrayList<Cancel>(0);

    private final CountDownLatch listenersFinished = new CountDownLatch(1);

    protected final Object lock;

    // set the ready flag if operation completed
    protected boolean completed = false;

    // by default false, change in case of success. An unfinished operation is
    // always set to failed
    protected FutureType type = FutureType.INIT;

    protected String reason = "unknown";

    private K self;

    private boolean cancel = false;

    /**
     * Default constructor that sets the lock object, which is used for synchronization to this instance.
     */
    public BaseFutureImpl() {
        this.lock = this;
    }

    /**
     * @param self2
     *            Set the type so that we are able to return it to the user. This is for making the API much more
     *            usable.
     */
    protected void self(final K self2) {
        this.self = self2;
    }
    
    /**
     * @return The object that stored this object. This is necessary for the builder pattern when using generics.
     */
    protected K self() {
        return self;
    }

    @Override
    public K await() throws InterruptedException {
        synchronized (lock) {
            checkDeadlock();
            while (!completed) {
                lock.wait();
            }
            return self;
        }
    }

    @Override
    public K awaitUninterruptibly() {
        synchronized (lock) {
            checkDeadlock();
            while (!completed) {
                try {
                    lock.wait();
                } catch (final InterruptedException e) {
                   LOG.debug("interrupted, but ignoring", e);
                }
            }
            return self;
        }
    }

    @Override
    public boolean await(final long timeoutMillis) throws InterruptedException {
        return await0(timeoutMillis, true);
    }

    @Override
    public boolean awaitUninterruptibly(final long timeoutMillis) {
        try {
            return await0(timeoutMillis, false);
        } catch (final InterruptedException e) {
            throw new RuntimeException("This should never ever happen.");
        }
    }

    /**
     * Internal await operation that also checks for potential deadlocks.
     * 
     * @param timeoutMillis
     *            The time to wait
     * @param interrupt
     *            Flag to indicate if the method can throw an InterruptedException
     * @return True if this future has finished in timeoutMillis time, false otherwise
     * @throws InterruptedException
     *             If the flag interrupt is true and this thread has been interrupted.
     */
    private boolean await0(final long timeoutMillis, final boolean interrupt) throws InterruptedException {
        final long startTime = (timeoutMillis <= 0) ? 0 : System.currentTimeMillis();
        long waitTime = timeoutMillis;
        synchronized (lock) {
            if (completed) {
                return completed;
            } else if (waitTime <= 0) {
                return completed;
            }
            checkDeadlock();
            while (true) {
                try {
                    lock.wait(waitTime);
                } catch (final InterruptedException e) {
                    if (interrupt) {
                        throw e;
                    }
                }
                if (completed) {
                    return true;
                } else {
                    waitTime = timeoutMillis - (System.currentTimeMillis() - startTime);
                    if (waitTime <= 0) {
                        return completed;
                    }
                }
            }
        }
    }

    @Override
    public boolean isCompleted() {
        synchronized (lock) {
            return completed;
        }
    }

    @Override
    public boolean isSuccess() {
        synchronized (lock) {
            return completed && (type == FutureType.OK);
        }
    }

    @Override
    public boolean isFailed() {
        synchronized (lock) {
            // failed means failed or canceled
            return completed && (type != FutureType.OK);
        }
    }

    @Override
    public K failed(final BaseFuture origin) {
        return failed(origin.failedReason());
    }

    @Override
    public K failed(final String failed, final BaseFuture origin) {
        StringBuilder sb = new StringBuilder(failed);
        return failed(sb.append(" <-> ").append(origin.failedReason()).toString());
    }

    @Override
    public K failed(final Throwable t) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        t.printStackTrace(printWriter);
        return failed(stringWriter.toString());
    }

    @Override
    public K failed(final String failed, final Throwable t) {
        if (t == null) {
            return failed("n/a");
        }
        StringBuilder sb = new StringBuilder(failed);
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        t.printStackTrace(printWriter);
        return failed(sb.append(" <-> ").append(stringWriter.toString()).toString());
    }

    @Override
    public K failed(final String failed) {
        synchronized (lock) {
            if (!completedAndNotify()) {
                return self;
            }
            this.reason = failed;
            this.type = FutureType.FAILED;
        }
        notifyListeners();
        return self;
    }

    @Override
    public String failedReason() {
        final StringBuffer sb = new StringBuffer("Future (compl/canc):");
        synchronized (lock) {
            sb.append(completed).append("/")
            	.append(cancel).append(", ").append(type.name())
            	.append(", ").append(reason);
            return sb.toString();
        }
    }

    @Override
    public FutureType type() {
        synchronized (lock) {
            return type;
        }
    }

    /**
     * Make sure that the calling method has synchronized (lock).
     * 
     * @return True if notified. It will notify if completed is not set yet.
     */
    protected boolean completedAndNotify() {
        if (!completed) {
            completed = true;
            lock.notifyAll();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public K awaitListeners() throws InterruptedException {
    	boolean wait = false;
    	synchronized (lock) {
    		if(listeners.size() > 0) {
    			wait = true;
    		}
    	}
    	if(wait) {
    		listenersFinished.await();
    	}
        return self;
    }
    
    @Override
    public K awaitListenersUninterruptibly() {
    	boolean wait = false;
    	synchronized (lock) {
    		if(listeners.size() > 0) {
    			wait = true;
    		}
    	}
    	while(wait) {
    		try {
    			listenersFinished.await();
    			wait = false;
    		} catch (InterruptedException e) {
                LOG.debug("interrupted, but ignoring", e);
            }
    	}
        return self;
    }
    
    @Override
    public K addListener(final BaseFutureListener<? extends BaseFuture> listener) {
        boolean notifyNow = false;
        synchronized (lock) {
            if (completed) {
                notifyNow = true;
            } else {
                listeners.add(listener);
            }
        }
        // called only once
        if (notifyNow) {
            callOperationComplete(listener);
        }
        return self;
    }

    /**
     * Call operation complete or call fail listener. If the fail listener fails, its printed as a stack trace.
     * 
     * @param listener
     *            The listener to call
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void callOperationComplete(final BaseFutureListener listener) {
        try {
            listener.operationComplete(this);
        } catch (final Exception e) {
            try {
                listener.exceptionCaught(e);
                listener.operationComplete(this);
            } catch (final Exception e1) {
                LOG.error("Unexcpected exception in exceptionCaught()", e1);
            }
        }
    }

    /**
     * If we block from a Netty thread, then the Netty thread won't receive any IO operations, thus make the user aware
     * of this situation.
     */
    private void checkDeadlock() {
        String currentName = Thread.currentThread().getName();
        if (currentName.startsWith(ConnectionBean.THREAD_NAME)) {
            throw new IllegalStateException("await*() in Netty I/O thread causes a dead lock or "
                    + "sudden performance drop. Use addListener() instead or "
                    + "call await*() from a different thread.");
        }
    }

    /**
     * Always call this from outside synchronized(lock)!
     */
    protected void notifyListeners() {
        // if this is synchronized, it will deadlock, so do not lock this!
        // There won't be any visibility problem or concurrent modification
        // because 'ready' flag will be checked against both addListener and
        // removeListener calls.
        for (final BaseFutureListener<? extends BaseFuture> listener : listeners) {
            callOperationComplete(listener);
        }
        listenersFinished.countDown();
        listeners.clear();
        // all events are one time events. It cannot happen that you get
        // notified twice
    }

    @Override
    public K removeListener(final BaseFutureListener<? extends BaseFuture> listener) {
        synchronized (lock) {
            if (!completed) {
                listeners.remove(listener);
            }
        }
        return self;
    }

    @Override
    public K addCancel(final Cancel cancelListener) {
        synchronized (cancels) {
            if (cancel) {
                cancelListener.cancel();
            }
            else {
                cancels.add(cancelListener);
            }
        }
        return self;
    }
    
    @Override
    public K removeCancel(final Cancel cancelListener) {
        synchronized (cancels) {
            if (!cancel) {
                cancels.remove(cancelListener);
            }
        }
        return self;
    }

    @Override
    public void cancel() {
        boolean notifyCancel = false;
        synchronized (cancels) {
            if (!cancel) {
                cancel = true;
                notifyCancel = true;
            } else {
                return;
            }
        }
        if (notifyCancel) {
            for (final Cancel cancellable : cancels) {
                cancellable.cancel();
            }
        }
    }
}
