package net.tomp2p.futures;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Futures {

    public static <K extends BaseFuture> FutureDone<K[]> whenAll(final K... all) {
        final AtomicInteger counter = new AtomicInteger();
        final int size = all.length;
        final FutureDone<K[]> futureDone = new FutureDone<K[]>();

        for (final K future : all) {
            future.addListener(new BaseFutureAdapter<K>() {
                @Override
                public void operationComplete(final K future) throws Exception {
                    if (counter.incrementAndGet() == size) {
                        futureDone.done(all);
                    }
                }
            });
        }

        return futureDone;
    }

    public static <K extends BaseFuture> FutureDone<List<K>> whenAll(final List<K> all) {
        final AtomicInteger counter = new AtomicInteger();
        final int size = all.size();
        final FutureDone<List<K>> futureDone = new FutureDone<List<K>>();

        for (final K future : all) {
            future.addListener(new BaseFutureAdapter<K>() {
                @Override
                public void operationComplete(final K future) throws Exception {
                    if (counter.incrementAndGet() == size) {
                        futureDone.done(all);
                    }
                }
            });
        }

        return futureDone;
    }

    public static <K extends BaseFuture> FutureDone<List<K>> whenAllSuccess(final List<K> all) {
        final AtomicInteger counter = new AtomicInteger();
        final int size = all.size();
        final FutureDone<List<K>> futureDone = new FutureDone<List<K>>();

        for (final K future : all) {
            future.addListener(new BaseFutureAdapter<K>() {
                @Override
                public void operationComplete(final K future) throws Exception {
                    if (futureDone.isCompleted()) {
                        return;
                    }
                    if (future.isFailed()) {
                        // at least one future failed
                        futureDone.failed(future);
                    } else if (counter.incrementAndGet() == size) {
                        futureDone.done(all);
                    }
                }
            });
        }

        return futureDone;
    }

    public static <K extends BaseFuture> FutureDone<K[]> whenAllSuccess(final K... all) {
        final AtomicInteger counter = new AtomicInteger();
        final int size = all.length;
        final FutureDone<K[]> futureDone = new FutureDone<K[]>();

        for (final K future : all) {
            future.addListener(new BaseFutureAdapter<K>() {
                @Override
                public void operationComplete(final K future) throws Exception {
                    if (futureDone.isCompleted()) {
                        return;
                    }
                    if (future.isFailed()) {
                        // at least one future failed
                        futureDone.failed(future);
                    } else if (counter.incrementAndGet() == size) {
                        futureDone.done(all);
                    }
                }
            });
        }

        return futureDone;
    }

    public static <K extends BaseFuture> FutureDone<K> whenAny(final List<K> all) {
        final FutureDone<K> futureDone = new FutureDone<K>();

        for (final K future : all) {
            future.addListener(new BaseFutureAdapter<K>() {
                @Override
                public void operationComplete(final K future) throws Exception {
                    futureDone.done(future);
                }
            });
        }

        return futureDone;
    }

    public static <K extends BaseFuture> FutureDone<K> whenAny(final K... all) {
        final FutureDone<K> futureDone = new FutureDone<K>();

        for (final K future : all) {
            future.addListener(new BaseFutureAdapter<K>() {
                @Override
                public void operationComplete(final K future) throws Exception {
                    futureDone.done(future);
                }
            });
        }

        return futureDone;
    }

    public static <K extends BaseFuture> FutureDone<K> whenAnySuccess(final List<K> all) {
        final AtomicInteger counter = new AtomicInteger();
        final int size = all.size();
        final FutureDone<K> futureDone = new FutureDone<K>();

        for (final K future : all) {
            future.addListener(new BaseFutureAdapter<K>() {
                @Override
                public void operationComplete(final K future) throws Exception {
                    if (futureDone.isCompleted()) {
                        return;
                    }
                    if (future.isSuccess()) {
                        futureDone.done(future);
                    } else if (counter.incrementAndGet() == size) {
                        futureDone.failed("not a single future was successful");
                    }
                }
            });
        }

        return futureDone;
    }

    public static <K extends BaseFuture> FutureDone<K> whenAnySuccess(final K... all) {
        final AtomicInteger counter = new AtomicInteger();
        final int size = all.length;
        final FutureDone<K> futureDone = new FutureDone<K>();

        for (final K future : all) {
            future.addListener(new BaseFutureAdapter<K>() {
                @Override
                public void operationComplete(final K future) throws Exception {
                    if (futureDone.isCompleted()) {
                        return;
                    }
                    if (future.isSuccess()) {
                        futureDone.done(future);
                    } else if (counter.incrementAndGet() == size) {
                        futureDone.failed("not a single future was successful");
                    }
                }
            });
        }

        return futureDone;
    }
}
