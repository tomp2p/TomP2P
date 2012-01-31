/*
 * Copyright 2012 Thomas Bocek
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureLateJoin;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.FutureRunnable;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerStatusListener;
import net.tomp2p.rpc.HandshakeRPC;
import net.tomp2p.utils.Timings;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Scheduler
{
	private static final Logger logger = LoggerFactory.getLogger(Scheduler.class);
	private static final int NR_THREADS = Runtime.getRuntime().availableProcessors() + 1;
	private static final int WARNING_THRESHOLD = 10000;
	private final LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
	private final ExecutorService executor = new ThreadPoolExecutor(NR_THREADS, NR_THREADS, 0L,
			TimeUnit.MILLISECONDS, queue, new MyThreadFactory());
	private volatile Maintenance maintenance;

	public void addQueue(FutureRunnable futureRunnable)
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("we are called from a TCP netty thread, so send this in an other thread "
					+ Thread.currentThread().getName()+". The queue size is: "+queue.size());
		}
		if(queue.size() > WARNING_THRESHOLD)
		{
			if(logger.isInfoEnabled())
			{
				logger.info("slow down, we have a huge backlog!");
			}
		}
		if(executor.isShutdown())
		{
			futureRunnable.failed("shutting down");
			return;
		}
		executor.execute(futureRunnable);
	}
	
	public void shutdown()
	{
		List<Runnable> runners = executor.shutdownNow();
		for (Runnable runner : runners)
		{
			FutureRunnable futureRunnable = (FutureRunnable) runner;
			futureRunnable.failed("Shutting down...");
		}
		if(maintenance!=null)
		{
			maintenance.shutdown();
		}
	}
	
	private class MyThreadFactory implements ThreadFactory 
	{
		private int nr = 0;
		public Thread newThread(Runnable r) 
		{
		     Thread t = new Thread(r, "scheduler-" + nr);
		     nr++;
		     return t;
		 }
	}
	
	private class Maintenance extends Thread implements Runnable
	{
		private volatile boolean running = true;
		final private List<PeerMap> peerMaps = new ArrayList<PeerMap>();
		final private HandshakeRPC handshakeRPC;
		final private ConnectionReservation connectionReservation;
		final private PeerMap masterPeerMap;
		final private int max;
		public Maintenance(PeerMap peerMap, HandshakeRPC handshakeRPC, ConnectionReservation connectionReservation, int max)
		{
			this.handshakeRPC = handshakeRPC;
			this.connectionReservation = connectionReservation;
			this.max = max;
			this.masterPeerMap = peerMap;
			add(peerMap);
			setName("maintenance");
		}
		
		@Override
		public void run()
		{
			Collection<PeerAddress> checkPeers = new ArrayList<PeerAddress>();
			while(running)
			{
				synchronized (peerMaps)
				{
					if(checkPeers.size()==0)
					{
						for(PeerMap peerMap: peerMaps)
						{
							checkPeers.addAll(peerMap.peersForMaintenance());
						}
					}
				}
				int i=0;
				final int max2 = Math.min(max, checkPeers.size());
				final FutureLateJoin<FutureResponse> lateJoin = new FutureLateJoin<FutureResponse>(max2);
				for(Iterator<PeerAddress> iterator = checkPeers.iterator(); i<max2;i++)
				{
					final PeerAddress peerAddress = iterator.next();
					FutureChannelCreator fcc = connectionReservation.reserve(1);
					fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>()
					{
						@Override
						public void operationComplete(FutureChannelCreator future) throws Exception
						{
							if(future.isSuccess())
							{
								ChannelCreator cc = future.getChannelCreator();
								FutureResponse futureResponse = handshakeRPC.pingUDP(peerAddress, cc);
								Utils.addReleaseListener(futureResponse, connectionReservation, cc, 1);
								lateJoin.add(futureResponse);
							}
						}
					});
					iterator.remove();
				}
				try
				{	
					lateJoin.await();
					Timings.sleep(1000);
				}
				catch (InterruptedException e)
				{
					lateJoin.setFailed("interrupted");
				}
			}
		}
		public void shutdown()
		{
			running = false;
			interrupt();
		}
		public void add(PeerMap peerMap)
		{
			synchronized (peerMaps)
			{
				peerMaps.add(peerMap);
			}
		}

		public PeerMap getMasterPeerMap()
		{
			return masterPeerMap;
		}
	}

	public void startMaintainance(final PeerMap peerMap, HandshakeRPC handshakeRPC, ConnectionReservation connectionReservation, int max)
	{
		if(maintenance == null)
		{
			maintenance = new Maintenance(peerMap, handshakeRPC, connectionReservation, max);
			maintenance.start();
		}
		else
		{
			maintenance.add(peerMap);
			maintenance.getMasterPeerMap().addPeerOfflineListener(new PeerStatusListener()
			{
				@Override
				public void peerOnline(PeerAddress peerAddress)
				{
					//we are interested in only those that are in our peer-map
					if(peerMap.contains(peerAddress))
					{
						peerMap.peerFound(peerAddress, null);
					}
				}
				
				@Override
				public void peerOffline(PeerAddress peerAddress, Reason reason)
				{
					//peerFail will eventually come to an peerOffline with reason NOT_REACHABLE
				}
				
				@Override
				public void peerFail(PeerAddress peerAddress, boolean force)
				{
					peerMap.peerOffline(peerAddress, force);
				}
			});
		}
	}
}
