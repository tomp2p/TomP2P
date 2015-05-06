/*
 * Copyright 2013 Thomas Bocek
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

package net.tomp2p.connection;

import io.netty.channel.EventLoopGroup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.p2p.RequestConfiguration;
import net.tomp2p.p2p.RoutingConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reserves a block of connections.
 * 
 * @author Thomas Bocek
 * 
 */
public class Reservation {
	
	private static final Logger LOG = LoggerFactory.getLogger(Reservation.class);

	private final int maxPermitsUDP;
	private final int maxPermitsTCP;
	private final int maxPermitsPermanentTCP;

	private final Semaphore semaphoreUPD;
	private final Semaphore semaphoreTCP;
	private final Semaphore semaphorePermanentTCP;

	private final ChannelClientConfiguration channelClientConfiguration;

	private final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
	// single thread
	private final ExecutorService executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, queue);
	private final EventLoopGroup workerGroup;

	// we should be fair, otherwise we see connection timeouts due to unfairness
	// if busy
	private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
	private final Lock read = readWriteLock.readLock();
	private final Lock write = readWriteLock.writeLock();
	private boolean shutdown = false;
	private final Collection<ChannelCreator> channelCreators = Collections
	        .synchronizedList(new ArrayList<ChannelCreator>());

	private final FutureDone<Void> futureReservationDone = new FutureDone<Void>();

	/**
	 * Creates a new reservation class with the 3 permits contained in the provided configuration.
	 * 
	 * @param workerGroup
	 *            The worker group for both UDP and TCP channels. This will not
	 *            be shutdown in this class, you need to shutdown it outside.
	 * @param channelClientConfiguration
	 *            Sets maxPermitsUDP: the number of maximum short-lived UDP
	 *            connections, maxPermitsTCP: the number of maximum short-lived
	 *            TCP connections, maxPermitsPermanentTCP: the number of maximum
	 *            permanent TCP connections
	 */
	public Reservation(final EventLoopGroup workerGroup, final ChannelClientConfiguration channelClientConfiguration) {
		this.workerGroup = workerGroup;
		this.maxPermitsUDP = channelClientConfiguration.maxPermitsUDP();
		this.maxPermitsTCP = channelClientConfiguration.maxPermitsTCP();
		this.maxPermitsPermanentTCP = channelClientConfiguration.maxPermitsPermanentTCP();
		this.semaphoreUPD = new Semaphore(maxPermitsUDP);
		this.semaphoreTCP = new Semaphore(maxPermitsTCP);
		this.semaphorePermanentTCP = new Semaphore(maxPermitsPermanentTCP);
		this.channelClientConfiguration = channelClientConfiguration;
	}

	/**
	 * @return The pending number of requests that are scheduled but not
	 *         executed yet.
	 */
	public int pendingRequests() {
		return queue.size();
	}

	/**
	 * Calculates the number of required connections for routing and request messages.
	 * 
	 * @param routingConfiguration
	 *            Contains the number of routing requests in parallel
	 * @param requestP2PConfiguration
	 *            Contains the number of requests for P2P operations in parallel
	 * @param builder
	 *            The builder that tells us if we should use TCP or UDP
	 * @return The future channel creator
	 */
	public FutureChannelCreator create(final RoutingConfiguration routingConfiguration,
	        final RequestConfiguration requestP2PConfiguration, final DefaultConnectionConfiguration builder) {
		if (routingConfiguration == null && requestP2PConfiguration == null) {
			throw new IllegalArgumentException("Both routing configuration and request configuration must be set.");
		}

		int nrConnectionsTCP = 0;
		int nrConnectionsUDP = 0;
		if (requestP2PConfiguration != null) {
			if (builder.isForceUDP()) {
				nrConnectionsUDP = requestP2PConfiguration.parallel();
			} else {
				nrConnectionsTCP = requestP2PConfiguration.parallel();
			}
		}
		if (routingConfiguration != null) {
			if (!builder.isForceTCP()) {
				nrConnectionsUDP = Math.max(nrConnectionsUDP, routingConfiguration.parallel());
			} else {
				nrConnectionsTCP = Math.max(nrConnectionsTCP, routingConfiguration.parallel());
			}
		}

		LOG.debug("Reservation UDP={}, TCP={}", nrConnectionsUDP, nrConnectionsTCP);
		return create(nrConnectionsUDP, nrConnectionsTCP);
	}

	/**
	 * Creates a channel creator for short-lived connections. Always call
	 * {@link ChannelCreator#shutdown()} to release all resources. This needs to
	 * be done in any case, whether FutureChannelCreator returns failed or
	 * success!
	 * 
	 * @param permitsUDP
	 *            The number of short-lived UDP connections
	 * @param permitsTCP
	 *            The number of short-lived TCP connections
	 * @return The future channel creator
	 */
	public FutureChannelCreator create(final int permitsUDP, final int permitsTCP) {
		if (permitsUDP > maxPermitsUDP) {
			throw new IllegalArgumentException(String.format("Cannot acquire more UDP connections (%s) than maximally allowed (%s).", permitsUDP, maxPermitsUDP));
		}
		if (permitsTCP > maxPermitsTCP) {
			throw new IllegalArgumentException(String.format("Cannot acquire more TCP connections (%s) than maximally allowed (%s).", permitsTCP, maxPermitsTCP));
		}
		final FutureChannelCreator futureChannelCreator = new FutureChannelCreator();
		read.lock();
		try {
			if (shutdown) {
				return futureChannelCreator.failed("Shutting down.");
			}

			FutureDone<Void> futureChannelCreationDone = new FutureDone<Void>();
			futureChannelCreationDone.addListener(new BaseFutureAdapter<FutureDone<Void>>() {
				@Override
				public void operationComplete(final FutureDone<Void> future) throws Exception {
					// release the permits in all cases
                    // otherwise, we may see inconsistencies
					semaphoreUPD.release(permitsUDP);
					semaphoreTCP.release(permitsTCP);
				}
			});
			executor.execute(new WaitReservation(futureChannelCreator, futureChannelCreationDone, permitsUDP,
			        permitsTCP));
			return futureChannelCreator;

		} finally {
			read.unlock();
		}
	}

	/**
	 * Creates a channel creator for permanent TCP connections.
	 * 
	 * @param permitsPermanentTCP
	 *            The number of long-lived TCP connections
	 * @return The future channel creator
	 */
	public FutureChannelCreator createPermanent(final int permitsPermanentTCP) {
		if (permitsPermanentTCP > maxPermitsPermanentTCP) {
			throw new IllegalArgumentException(String.format("Cannot acquire more permantent TCP connections (%s) than maximally allowed (%s).", permitsPermanentTCP, maxPermitsPermanentTCP));
		}
		final FutureChannelCreator futureChannelCreator = new FutureChannelCreator();
		read.lock();
		try {
			if (shutdown) {
				return futureChannelCreator.failed("shutting down");
			}
			FutureDone<Void> futureChannelCreationDone = new FutureDone<Void>();
			futureChannelCreationDone.addListener(new BaseFutureAdapter<FutureDone<Void>>() {
				@Override
				public void operationComplete(final FutureDone<Void> future) throws Exception {
					// release the permits in all cases
                    // otherwise, we may see inconsistencies
					semaphorePermanentTCP.release(permitsPermanentTCP);
				}
			});
			executor.execute(new WaitReservationPermanent(futureChannelCreator, futureChannelCreationDone,
			        permitsPermanentTCP));
			return futureChannelCreator;
		} finally {
			read.unlock();
		}
	}

	/**
	 * Shuts down all the channel creators.
	 * 
	 * @return The future when the shutdown is complete
	 */
	public FutureDone<Void> shutdown() {
		write.lock();
		try {
			if (shutdown) {
				return futureReservationDone.failed("Already shutting down");
			}
			shutdown = true;
		} finally {
			write.unlock();
		}

		// Fast shutdown for those that are in the queue is not required.
        // Let the executor finish since the shutdown-flag is set and the
        // future will be set as well to "shutting down".

		for (Runnable r : executor.shutdownNow()) {
			if (r instanceof WaitReservation) {
				WaitReservation wr = (WaitReservation) r;
				wr.futureChannelCreator().failed("Shutting down.");
			} else {
				WaitReservationPermanent wr = (WaitReservationPermanent) r;
				wr.futureChannelCreator().failed("Shutting down.");
			}
		}
		
		final Collection<ChannelCreator> copyChannelCreators;
		synchronized (channelCreators) {
			copyChannelCreators = new ArrayList<ChannelCreator>(channelCreators);
		}
		

		// the channelCreator does not change anymore from here on
		final int size = copyChannelCreators.size();
		if (size == 0) {
			futureReservationDone.done();
		} else {
			final AtomicInteger completeCounter = new AtomicInteger(0);
			for (final ChannelCreator channelCreator : copyChannelCreators) {
				// this is very important that we set first the listener and
				// then call shutdown. Otherwise, the order of
				// the listener calls is not guaranteed and we may call this
				// listener before the semphore.release,
				// causing an exception.
				channelCreator.shutdownFuture().addListener(new BaseFutureAdapter<FutureDone<Void>>() {
					@Override
					public void operationComplete(final FutureDone<Void> future) throws Exception {
						if (completeCounter.incrementAndGet() == size) {
							// we can block here as we block in
							// GlobalEventExecutor.INSTANCE
							semaphoreUPD.acquireUninterruptibly(maxPermitsUDP);
							semaphoreTCP.acquireUninterruptibly(maxPermitsTCP);
							semaphorePermanentTCP.acquireUninterruptibly(maxPermitsPermanentTCP);
							futureReservationDone.done();
						}
					}
				});
				channelCreator.shutdown();
			}
		}
		// wait for completion
		return futureReservationDone;
	}

	/**
	 * Adds a channel creator to the set and also adds it to the shutdown listener.
	 * 
	 * @param channelCreator
	 *            The channel creator
	 */
	private void addToSet(final ChannelCreator channelCreator) {
		channelCreator.shutdownFuture().addListener(new BaseFutureAdapter<FutureDone<Void>>() {
			@Override
			public void operationComplete(final FutureDone<Void> future) throws Exception {
				channelCreators.remove(channelCreator);
			}
		});
		channelCreators.add(channelCreator);
	}

	/**
	 * Tries to reserve a channel creator. If too many channels already created,
	 * wait until channels are closed. This waiter is for the short-lived
	 * connections.
	 * 
	 * @author Thomas Bocek
	 * 
	 */
	private class WaitReservation implements Runnable {
		private final FutureChannelCreator futureChannelCreator;
		private final FutureDone<Void> futureChannelCreationShutdown;
		private final int permitsUDP;
		private final int permitsTCP;

		/**
		 * Creates a reservation that returns a {@link ChannelCreator} in a
		 * future once we have the semaphore.
		 * 
		 * @param futureChannelCreator
		 *            The status of the creating
		 * @param futureChannelCreationShutdown
		 *            The {@link ChannelCreator} shutdown feature needs to be
		 *            passed since we need it for {@link ChannelCreator#shutdown()}.
		 * @param permitsUDP
		 *            The number of permits for UDP
		 * @param permitsTCP
		 *            The number of permits for TCP
		 */
		public WaitReservation(final FutureChannelCreator futureChannelCreator,
		        final FutureDone<Void> futureChannelCreationShutdown, final int permitsUDP, final int permitsTCP) {
			this.futureChannelCreator = futureChannelCreator;
			this.futureChannelCreationShutdown = futureChannelCreationShutdown;
			this.permitsUDP = permitsUDP;
			this.permitsTCP = permitsTCP;
		}

		@Override
		public void run() {
			final ChannelCreator channelCreator;
			read.lock();
			try {
				if (shutdown) {
					futureChannelCreator.failed("shutting down");
					return;
				}

				try {
					semaphoreUPD.acquire(permitsUDP);
				} catch (InterruptedException e) {
					futureChannelCreator.failed(e);
					return;
				}
				try {
					semaphoreTCP.acquire(permitsTCP);
				} catch (InterruptedException e) {
					semaphoreUPD.release(permitsUDP);
					futureChannelCreator.failed(e);
					return;
				}

				channelCreator = new ChannelCreator(workerGroup, futureChannelCreationShutdown, permitsUDP, permitsTCP,
				        channelClientConfiguration);
				addToSet(channelCreator);
			} finally {
				read.unlock();
			}
			futureChannelCreator.reserved(channelCreator);
		}

		private FutureChannelCreator futureChannelCreator() {
			return futureChannelCreator;
		}

	}

	/**
	 * Tries to reserve a channel creator. If too many channel already created,
	 * wait until channels are closed. This waiter is for the long-lived
	 * connections.
	 * 
	 * @author Thomas Bocek
	 * 
	 */
	private final class WaitReservationPermanent implements Runnable {
		private final FutureChannelCreator futureChannelCreator;
		private final FutureDone<Void> futureChannelCreationShutdown;
		private final int permitsPermanentTCP;

		/**
		 * Creates a reservation that returns a {@link ChannelCreator} in a
		 * future once we have the semaphore.
		 * 
		 * @param futureChannelCreator
		 *            The status of the creating
		 * @param futureChannelCreationShutdown
		 *            The {@link ChannelCreator} shutdown feature needs to be
		 *            passed since we need it for {@link Reservation#shutdown()}
		 *            .
		 * @param permitsPermanentTCP
		 *            The number of permits
		 */
		private WaitReservationPermanent(final FutureChannelCreator futureChannelCreator,
		        final FutureDone<Void> futureChannelCreationShutdown, final int permitsPermanentTCP) {
			this.futureChannelCreator = futureChannelCreator;
			this.futureChannelCreationShutdown = futureChannelCreationShutdown;
			this.permitsPermanentTCP = permitsPermanentTCP;
		}

		@Override
		public void run() {
			ChannelCreator channelCreator;
			read.lock();
			try {
				if (shutdown) {
					futureChannelCreator.failed("shutting down");
					return;
				}

				try {
					semaphorePermanentTCP.acquire(permitsPermanentTCP);
				} catch (InterruptedException e) {
					futureChannelCreator.failed(e);
					return;
				}

				channelCreator = new ChannelCreator(workerGroup, futureChannelCreationShutdown, 0, permitsPermanentTCP,
				        channelClientConfiguration);
				addToSet(channelCreator);
			} finally {
				read.unlock();
			}
			futureChannelCreator.reserved(channelCreator);
		}

		private FutureChannelCreator futureChannelCreator() {
			return futureChannelCreator;
		}
	}

}
