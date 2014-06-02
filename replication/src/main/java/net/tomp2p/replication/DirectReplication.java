package net.tomp2p.replication;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.p2p.AutomaticFuture;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.Shutdown;
import net.tomp2p.p2p.builder.Builder;

public class DirectReplication implements Shutdown {

	final private ScheduledExecutorService scheduledExecutorService;
	final private Peer peer;
	final private AtomicInteger runnerCounter = new AtomicInteger(0);
	final private FutureDone<Void> shutdownFuture = new FutureDone<Void>();

	private boolean shutdown = false;

	final private class DirectReplicationWorker implements Runnable {

		private int counter = 0;
		private ScheduledFuture<?> future;
		final int repetitions;
		final AutomaticFuture automaticFuture;
		final Builder builder;
		final CountDownLatch latch;

		private DirectReplicationWorker(final Builder builder, final AutomaticFuture automaticFuture,
		        final int repetitions, final CountDownLatch latch) {
			runnerCounter.incrementAndGet();
			this.builder = builder;
			this.repetitions = repetitions;
			this.automaticFuture = automaticFuture;
			this.latch = latch;
		}

		@Override
		public void run() {
			boolean shutdownNow = false;
			synchronized (DirectReplication.this) {
				shutdownNow = shutdown;
            }
			try {
				if (shutdownNow || ( !(repetitions<0) && counter++ > repetitions)) {
					shutdown();
				} else {
					BaseFuture baseFuture = builder.start();
					peer.notifyAutomaticFutures(baseFuture);
					if (automaticFuture != null) {
						automaticFuture.futureCreated(baseFuture);
					}
				}
			} catch (Throwable t) {
				t.printStackTrace();
			}
		}

		private void shutdown() {
			// wait until future is set
			try {
				latch.await();
			} catch (InterruptedException e) {
				// try anyway
			}
			if (future != null) {
				future.cancel(false);
			}
			if (runnerCounter.decrementAndGet() == 0) {
				shutdownFuture.done();
			}
		}
	};

	public DirectReplication(Peer peer) {
		this(peer, 1, Executors.defaultThreadFactory());
	}

	public DirectReplication(Peer peer, int corePoolSize) {
		this(peer, corePoolSize, Executors.defaultThreadFactory());
	}

	public DirectReplication(Peer peer, int corePoolSize, ThreadFactory threadFactory) {
		this.scheduledExecutorService = Executors.newScheduledThreadPool(corePoolSize, threadFactory);
		this.peer = peer;
		peer.addShutdownListener(this);
	}

	public Shutdown direct(final Builder builder, final int intervalMillis, final int repetitions) {
		return direct(builder, intervalMillis, repetitions, null);
	}

	public Shutdown direct(final Builder builder, final int intervalMillis, final int repetitions,
	        final AutomaticFuture automaticFuture) {
		synchronized (this) {
			if (shutdown) {
				return null;
			}
			final CountDownLatch latch = new CountDownLatch(1);
			final DirectReplicationWorker worker = new DirectReplicationWorker(builder, automaticFuture, repetitions, latch);
			ScheduledFuture<?> future = scheduledExecutorService.scheduleWithFixedDelay(worker, 0, intervalMillis,
			        TimeUnit.MILLISECONDS);
			worker.future = future;
			latch.countDown();
			return new Shutdown() {
				@Override
				public BaseFuture shutdown() {
					worker.shutdown();
					return new FutureDone<Void>().done();
				}
			};
		}
	}

	public FutureDone<Void> shutdown() {
		synchronized (this) {
			shutdown = true;
		}
		for (Runnable worker : scheduledExecutorService.shutdownNow()) {
			//running them will cause a shutdown if the flag is set
			worker.run();
		}
		peer.removeShutdownListener(this);
		return shutdownFuture;
	}
}
