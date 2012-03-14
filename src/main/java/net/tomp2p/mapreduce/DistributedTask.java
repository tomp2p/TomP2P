package net.tomp2p.mapreduce;

import java.security.KeyPair;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionReservation;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.futures.FutureForkJoin;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.FutureRouting;
import net.tomp2p.futures.FutureTask;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.DistributedHashMap;
import net.tomp2p.p2p.DistributedRouting;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.p2p.TaskConfiguration;
import net.tomp2p.p2p.DistributedHashMap.Operation;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DigestInfo;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Utils;

public class DistributedTask
{
	final private static Logger logger = LoggerFactory.getLogger(DistributedTask.class);
	final private DistributedRouting routing;
	final private AsyncTask asyncTask;
	
	public DistributedTask(DistributedRouting routing, AsyncTask asyncTask)
	{
		this.routing = routing;
		this.asyncTask = asyncTask;
	}
	
	/**
	 * Submit a task to the DHT. The node that is close to the locationKey will
	 * get the task. The routing process returns a list of close peers with the
	 * current load. The peer with the lowest load will get the task.
	 * 
	 * @param locationKey
	 * @param dataMap
	 * @param routingConfiguration
	 * @param taskConfiguration
	 * @param futureChannelCreator
	 * @param signMessage
	 * @param isAutomaticCleanup
	 * @param connectionReservation
	 * @return
	 */
	public FutureTask submit(final Number160 locationKey, final Map<Number160, Data> dataMap, final Worker worker,
			final RoutingConfiguration routingConfiguration, final TaskConfiguration taskConfiguration, 
			final FutureChannelCreator futureChannelCreator,  final boolean signMessage, final boolean isAutomaticCleanup,
			final ConnectionReservation connectionReservation)
	{
		final FutureTask futureTask = new FutureTask();
		futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>()
		{
			@Override
			public void operationComplete(FutureChannelCreator future) throws Exception
			{
				if(future.isSuccess())
				{
					final ChannelCreator channelCreator = future.getChannelCreator();
					//routing, find close peers
					final FutureRouting futureRouting = createRouting(locationKey, null, null,
							routingConfiguration, taskConfiguration, Type.REQUEST_4, channelCreator);
					futureRouting.addListener(new BaseFutureAdapter<FutureRouting>()
					{
						@Override
						public void operationComplete(FutureRouting future) throws Exception
						{
							if (futureRouting.isSuccess())
							{
								SortedMap<PeerAddress, DigestInfo> map = future.getDirectHitsDigest();
								NavigableSet<PeerAddress> queue = findBest(map);
								parallelRequests(futureTask, queue, taskConfiguration, channelCreator, locationKey, dataMap, 
										worker, taskConfiguration.isForceUPD(), taskConfiguration.isSign());
							}
							else
							{
								futureTask.setFailed(futureRouting);
							}
						}
					});
				}
				else
				{
					futureTask.setFailed(future);
				}
			}
		});
		return futureTask;
	}
	
	private void parallelRequests(FutureTask futureTask, NavigableSet<PeerAddress> queue, 
			TaskConfiguration taskConfiguration, ChannelCreator channelCreator, Number160 taskId, 
			Map<Number160, Data> dataMap, Worker worker, boolean forceUDP, boolean sign)
	{
		FutureAsyncTask[] futures = new FutureAsyncTask[taskConfiguration.getParallel()];
		loopRec(queue, taskConfiguration.getMinimumResults(), new AtomicInteger(0), taskConfiguration
				.getMaxFailure(), taskConfiguration.getParallelDiff(), futures, futureTask,
				true, channelCreator, taskId, dataMap, worker, forceUDP, sign);
	}
	
	private void loopRec(final NavigableSet<PeerAddress> queue, final int min,
			final AtomicInteger nrFailure, final int maxFailure, final int parallelDiff,
			final FutureAsyncTask[] futures, final FutureTask futureTask,
			final boolean cancelOnFinish, final ChannelCreator channelCreator,
			final Number160 taskId, final Map<Number160, Data> dataMap, 
			final Worker mapper, final boolean forceUDP, final boolean sign)
	{
		int active = 0;
		for (int i = 0; i < min + parallelDiff; i++)
		{
			if (futures[i] == null)
			{
				PeerAddress next = queue.pollFirst();
				if (next != null)
				{
					active++;
					futures[i] = asyncTask.submit(next, channelCreator, taskId, dataMap, mapper, forceUDP, sign);	
					futureTask.addRequests(futures[i]);
				}
			}
			else
				active++;
		}
		if (active == 0)
		{
			futureTask.setDone();
			DistributedRouting.cancel(cancelOnFinish, min + parallelDiff, futures);
			return;
		}
		if (logger.isDebugEnabled())
		{
			logger.debug("fork/join status: " + min + "/" + active + " (" + parallelDiff + ")");
		}
		FutureForkJoin<FutureAsyncTask> fp = new FutureForkJoin<FutureAsyncTask>(Math
				.min(min, active), false, futures);
		fp.addListener(new BaseFutureAdapter<FutureForkJoin<FutureAsyncTask>>()
		{
			@Override
			public void operationComplete(FutureForkJoin<FutureAsyncTask> future) throws Exception
			{
				for (FutureAsyncTask futureAsyncTask : future.getCompleted())
				{
					futureTask.setProgress(futureAsyncTask);
				}
				// we are finished if forkjoin says so or we got too many
				// failures
				if (future.isSuccess() || nrFailure.incrementAndGet() > maxFailure)
				{
					if (cancelOnFinish)
					{
						DistributedRouting.cancel(cancelOnFinish, min + parallelDiff, futures);
					}
					futureTask.setDone();
				}
				else
				{
					loopRec(queue, min - future.getSuccessCounter(), nrFailure, maxFailure,
							parallelDiff, futures, futureTask, cancelOnFinish, channelCreator, 
							taskId, dataMap, mapper, forceUDP, sign);
				}
			}
		});
	}

	
	private FutureRouting createRouting(Number160 locationKey, Number160 domainKey,
			Set<Number160> contentKeys, RoutingConfiguration routingConfiguration,
			TaskConfiguration taskConfiguration, Type type, ChannelCreator channelCreator)
	{
		return routing.route(locationKey, domainKey, contentKeys, type, routingConfiguration
				.getDirectHits(), routingConfiguration.getMaxNoNewInfo(taskConfiguration
				.getMinimumResults()), routingConfiguration.getMaxFailures(),
				routingConfiguration.getMaxSuccess(),
				routingConfiguration.getParallel(), routingConfiguration.isForceTCP(),
				channelCreator);
	}
	
	static NavigableSet<PeerAddress> findBest(SortedMap<PeerAddress, DigestInfo> map)
	{
		// TODO Auto-generated method stub
		return null;
	}
}