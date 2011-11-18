/*
 * Copyright 2011 Thomas Bocek
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
package net.tomp2p.p2p;
import java.security.PublicKey;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureCreate;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.futures.FutureData;
import net.tomp2p.futures.FutureForkJoin;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.FutureRouting;
import net.tomp2p.message.Message.Command;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DigestInfo;
import net.tomp2p.rpc.DirectDataRPC;
import net.tomp2p.rpc.StorageRPC;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Utils;

import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedHashHashMap
{
	final private static Logger logger = LoggerFactory.getLogger(DistributedHashHashMap.class);
	final private DistributedRouting routing;
	final private StorageRPC store;
	final private DirectDataRPC directDataRPC;

	public DistributedHashHashMap(DistributedRouting routing, StorageRPC store, DirectDataRPC directDataRPC)
	{
		this.routing = routing;
		this.store = store;
		this.directDataRPC = directDataRPC;
	}

	public FutureDHT add(final Number160 locationKey, final Number160 domainKey,
			final Collection<Data> dataSet, RoutingConfiguration routingConfiguration,
			final RequestP2PConfiguration p2pConfiguration, final boolean protectDomain,
			final boolean signMessage, final FutureCreate<FutureDHT> futureCreate, final ChannelCreator cc)
	{
		
		final FutureRouting futureRouting = createRouting(locationKey, domainKey, null,
				routingConfiguration, p2pConfiguration, Command.NEIGHBORS_STORAGE, false, cc);
		final FutureDHT futureDHT = new FutureDHT(p2pConfiguration.getMinimumResults(), new VotingSchemeDHT(), futureCreate, futureRouting);
		futureRouting.addListener(new BaseFutureAdapter<FutureRouting>()
		{
			@Override
			public void operationComplete(FutureRouting futureRouting) throws Exception
			{
				if (futureRouting.isSuccess())
				{
					if (logger.isDebugEnabled())
						logger.debug("adding lkey=" + locationKey + " on "
								+ futureRouting.getPotentialHits());
					loop(p2pConfiguration, futureRouting.getPotentialHits(), futureDHT, false,
							new Operation()
							{
								Map<PeerAddress, Collection<Number160>> rawData = new HashMap<PeerAddress, Collection<Number160>>();

								@Override
								public FutureResponse create(PeerAddress address)
								{
									return store.add(address, locationKey, domainKey, dataSet,
											protectDomain, signMessage, cc);
								}

								@Override
								public void response(FutureDHT futureDHT)
								{
									futureDHT.setStoredKeys(rawData, false);
								}

								@Override
								public void interMediateResponse(FutureResponse future)
								{
									rawData.put(future.getRequest().getRecipient(), future
											.getResponse().getKeys());
								}
							});
				}
				else
				{
					futureDHT.setFailed("routing failed");
				}
			}
		});
		return futureDHT;
	}

	public FutureDHT direct(final Number160 locationKey, final ChannelBuffer buffer,
			final boolean raw, RoutingConfiguration routingConfiguration,
			final RequestP2PConfiguration p2pConfiguration,
			final FutureCreate<FutureDHT> futureCreate, final boolean cancelOnFinish, final ChannelCreator cc)
	{
		
		final FutureRouting futureRouting = createRouting(locationKey, null, null, routingConfiguration,
				p2pConfiguration, Command.NEIGHBORS_STORAGE, false, cc);
		final FutureDHT futureDHT = new FutureDHT(p2pConfiguration.getMinimumResults(), new VotingSchemeDHT(), futureCreate, futureRouting);
		futureRouting.addListener(new BaseFutureAdapter<FutureRouting>()
		{
			@Override
			public void operationComplete(FutureRouting futureRouting) throws Exception
			{
				if (futureRouting.isSuccess())
				{
					if (logger.isDebugEnabled())
						logger.debug("storing lkey=" + locationKey + " on "
								+ futureRouting.getPotentialHits());
					loop(p2pConfiguration, futureRouting.getPotentialHits(), futureDHT,
							cancelOnFinish, new Operation()
							{
								Map<PeerAddress, ChannelBuffer> rawChannels = new HashMap<PeerAddress, ChannelBuffer>();
								Map<PeerAddress, Object> rawObjects = new HashMap<PeerAddress, Object>();

								@Override
								public FutureResponse create(PeerAddress address)
								{
									return directDataRPC.send(address, buffer, raw, cc);
								}

								@Override
								public void response(FutureDHT futureDHT)
								{
									if (raw)
										futureDHT.setDirectData1(rawChannels);
									else
										futureDHT.setDirectData2(rawObjects);
								}

								@Override
								public void interMediateResponse(FutureResponse future)
								{
									FutureData futureData = (FutureData) future;
									if (raw)
										rawChannels.put(future.getRequest().getRecipient(),
												futureData.getBuffer());
									else
										rawObjects.put(future.getRequest().getRecipient(),
												futureData.getObject());
								}
							});
				}
				else
				{
					futureDHT.setFailed("routing failed");
				}
			}
		});
		return futureDHT;
	}

	public FutureDHT put(final Number160 locationKey, final Number160 domainKey,
			final Map<Number160, Data> dataMap, RoutingConfiguration routingConfiguration,
			final RequestP2PConfiguration p2pConfiguration, final boolean putIfAbsent,
			final boolean protectDomain, final boolean signMessage,
			final FutureCreate<FutureDHT> futureCreate, final ChannelCreator cc)
	{
		final FutureRouting futureRouting = createRouting(locationKey, domainKey, null,
				routingConfiguration, p2pConfiguration, Command.NEIGHBORS_STORAGE, false, cc);
		final FutureDHT futureDHT = new FutureDHT(p2pConfiguration.getMinimumResults(), new VotingSchemeDHT(),futureCreate, futureRouting);
		futureRouting.addListener(new BaseFutureAdapter<FutureRouting>()
		{
			@Override
			public void operationComplete(FutureRouting futureRouting) throws Exception
			{
				if (futureRouting.isSuccess())
				{
					if (logger.isDebugEnabled())
						logger.debug("storing lkey=" + locationKey + " on "
								+ futureRouting.getPotentialHits());
					loop(p2pConfiguration, futureRouting.getPotentialHits(), futureDHT, false,
							new Operation()
							{
								Map<PeerAddress, Collection<Number160>> rawData = new HashMap<PeerAddress, Collection<Number160>>();

								@Override
								public FutureResponse create(PeerAddress address)
								{
									boolean protectEntry = Utils.checkEntryProtection(dataMap);
									return putIfAbsent ? store.putIfAbsent(address, locationKey,
											domainKey, dataMap, protectDomain, protectEntry, signMessage, cc) : store
											.put(address, locationKey, domainKey, dataMap,
													protectDomain, protectEntry, signMessage, cc);
								}

								@Override
								public void response(FutureDHT futureDHT)
								{
									futureDHT.setStoredKeys(rawData, putIfAbsent);
								}

								@Override
								public void interMediateResponse(FutureResponse future)
								{
									rawData.put(future.getRequest().getRecipient(), future
											.getResponse().getKeys());
								}
							});
				}
				else
				{
					futureDHT.setFailed("routing failed");
				}
			}
		});
		return futureDHT;
	}

	public FutureDHT get(final Number160 locationKey, final Number160 domainKey,
			final Set<Number160> contentKeys, final PublicKey publicKey,
			RoutingConfiguration routingConfiguration,
			final RequestP2PConfiguration p2pConfiguration,
			final EvaluatingSchemeDHT evaluationScheme, final boolean signMessage, final ChannelCreator cc)
	{
		final FutureRouting futureRouting = createRouting(locationKey, domainKey, contentKeys,
				routingConfiguration, p2pConfiguration, Command.NEIGHBORS_STORAGE, true, cc);
		final FutureDHT futureDHT = new FutureDHT(p2pConfiguration.getMinimumResults(), evaluationScheme, null, futureRouting);
		futureRouting.addListener(new BaseFutureAdapter<FutureRouting>()
		{
			@Override
			public void operationComplete(FutureRouting futureRouting) throws Exception
			{
				if (futureRouting.isSuccess())
				{
					if (logger.isDebugEnabled()) 
					{
						logger.debug("found direct hits for get: " + futureRouting.getDirectHits());
					}
					loop(adjustConfiguration(p2pConfiguration, futureRouting.getDirectHitsDigest()), futureRouting.getDirectHits(), futureDHT, true,
							new Operation()
							{
								Map<PeerAddress, Map<Number160, Data>> rawData = new HashMap<PeerAddress, Map<Number160, Data>>();

								@Override
								public FutureResponse create(PeerAddress address)
								{
									return store.get(address, locationKey, domainKey, contentKeys,
											publicKey, signMessage, cc);
								}

								@Override
								public void response(FutureDHT futureDHT)
								{
									futureDHT.setData(rawData);
								}

								@Override
								public void interMediateResponse(FutureResponse future)
								{
									rawData.put(future.getRequest().getRecipient(), future
											.getResponse().getDataMap());
								}
							});
				}
				else
					futureDHT.setFailed("routing failed");
			}
		});
		return futureDHT;
	}

	public FutureDHT remove(final Number160 locationKey, final Number160 domainKey,
			final Set<Number160> contentKeys, RoutingConfiguration routingConfiguration,
			final RequestP2PConfiguration p2pConfiguration, final boolean returnResults,
			final boolean signMessage, FutureCreate<FutureDHT> futureCreate, final ChannelCreator cc)
	{
		final FutureRouting futureRouting = createRouting(locationKey, domainKey, contentKeys,
				routingConfiguration, p2pConfiguration, Command.NEIGHBORS_STORAGE, true, cc);
		final FutureDHT futureDHT = new FutureDHT(p2pConfiguration.getMinimumResults(), new VotingSchemeDHT(), futureCreate, futureRouting);
		futureRouting.addListener(new BaseFutureAdapter<FutureRouting>()
		{
			@Override
			public void operationComplete(FutureRouting futureRouting) throws Exception
			{
				if (futureRouting.isSuccess())
				{
					if (logger.isDebugEnabled())
						logger.debug("found direct hits for remove: "
								+ futureRouting.getDirectHits());
					loop(p2pConfiguration, futureRouting.getDirectHits(), futureDHT, false,
							new Operation()
							{
								Map<PeerAddress, Map<Number160, Data>> rawDataResult = new HashMap<PeerAddress, Map<Number160, Data>>();
								Map<PeerAddress, Collection<Number160>> rawDataNoResult = new HashMap<PeerAddress, Collection<Number160>>();

								@Override
								public FutureResponse create(PeerAddress address)
								{
									return store.remove(address, locationKey, domainKey,
											contentKeys, returnResults, signMessage, cc);
								}

								@Override
								public void response(FutureDHT futureDHT)
								{
									if (returnResults)
										futureDHT.setData(rawDataResult);
									else
										futureDHT.setRemovedKeys(rawDataNoResult);
								}

								@Override
								public void interMediateResponse(FutureResponse future)
								{
									if (returnResults)
										rawDataResult.put(future.getRequest().getRecipient(),
												future.getResponse().getDataMap());
									else
										rawDataNoResult.put(future.getRequest().getRecipient(),
												future.getResponse().getKeys());
								}
							});
				}
				else
					futureDHT.setFailed("routing failed");
			}
		});
		return futureDHT;
	}

	private void loop(RequestP2PConfiguration p2pConfiguration, SortedSet<PeerAddress> queue,
			FutureDHT futureDHT, boolean cancleOnFinish, Operation operation)
	{
		if(p2pConfiguration.getMinimumResults() == 0)
		{
			operation.response(futureDHT);
			return;
		}
		FutureResponse[] futures = new FutureResponse[p2pConfiguration.getParallel()];
		// here we split min and pardiff, par=min+pardiff
		loopRec(queue, p2pConfiguration.getMinimumResults(), new AtomicInteger(0), p2pConfiguration
				.getMaxFailure(), p2pConfiguration.getParallelDiff(), futures, futureDHT,
				cancleOnFinish, operation);
	}

	private void loopRec(final SortedSet<PeerAddress> queue, final int min,
			final AtomicInteger nrFailure, final int maxFailure, final int parallelDiff,
			final FutureResponse[] futures, final FutureDHT futureDHT,
			final boolean cancelOnFinish, final Operation operation)
	{
		// final int parallel=min+parallelDiff;
		int active = 0;
		for (int i = 0; i < min + parallelDiff; i++)
		{
			if (futures[i] == null)
			{
				PeerAddress next = Utils.pollFirst(queue);
				// not available in java5
				// PeerAddress next = queue.pollFirst();
				if (next != null)
				{
					active++;
					futures[i] = operation.create(next);
				}
			}
			else
				active++;
		}
		if (active == 0)
		{
			operation.response(futureDHT);
			DistributedRouting.cancel(cancelOnFinish, min + parallelDiff, futures);
			return;
		}
		if (logger.isDebugEnabled())
			logger.debug("fork/join status: " + min + "/" + active + " (" + parallelDiff + ")");
		FutureForkJoin<FutureResponse> fp = new FutureForkJoin<FutureResponse>(Math
				.min(min, active), false, futures);
		fp.addListener(new BaseFutureAdapter<FutureForkJoin<FutureResponse>>()
		{
			@Override
			public void operationComplete(FutureForkJoin<FutureResponse> future) throws Exception
			{
				for (FutureResponse futureResponse : future.getAll())
				{
					if (futureResponse.isSuccess())
						operation.interMediateResponse(futureResponse);
				}
				// we are finished if forkjoin says so or we got too many failures
				if (future.isSuccess() || nrFailure.incrementAndGet() > maxFailure)
				{
					if(!cancelOnFinish)
					{
						for (FutureResponse futureResponse : future.getAll())
						{
							if (!futureResponse.isSuccess())
							{
								//we add pending futures that are not canceled that the user can wait for those futures
								futureDHT.addPending(futureResponse);
							}
						}
					}
					else
					{
						DistributedRouting.cancel(cancelOnFinish, min + parallelDiff, futures);
					}
					operation.response(futureDHT);
				}
				else
				{
					loopRec(queue, min - future.getSuccessCounter(), nrFailure, maxFailure,
							parallelDiff, futures, futureDHT, cancelOnFinish, operation);
				}
			}
		});
	}

	private FutureRouting createRouting(Number160 locationKey, Number160 domainKey,
			Set<Number160> contentKeys, RoutingConfiguration routingConfiguration,
			RequestP2PConfiguration p2pConfiguration, Command command, boolean isDirect, final ChannelCreator cc)
	{
		return routing.route(locationKey, domainKey, contentKeys, command, routingConfiguration
				.getDirectHits(), routingConfiguration.getMaxNoNewInfo(p2pConfiguration
				.getMinimumResults()), routingConfiguration.getMaxFailures(), routingConfiguration.getMaxSuccess(),
				routingConfiguration.getParallel(), isDirect, routingConfiguration.isForceSocket(), cc);
	}
	public interface Operation
	{
		public abstract FutureResponse create(PeerAddress address);

		public abstract void response(FutureDHT futureDHT);

		public abstract void interMediateResponse(FutureResponse futureResponse);
	}
	
	/**
	 * Adjusts the number of minimum requests in the P2P configuration. When we query x peers for the get() operation and they
	 * have y different data stored (y <= x), then set the minimum to y or to the value the user set if its smaller. If no data
	 * is found, then return 0, so we don't start P2P RPCs.
	 * 
	 * @param p2pConfiguration The old P2P configuration with the user specified minimum result
	 * @param directHitsDigest The digest information from the routing process
	 * @return The new RequestP2PConfiguration with the new minimum result
	 */
	public static RequestP2PConfiguration adjustConfiguration(RequestP2PConfiguration p2pConfiguration, 
			SortedMap<PeerAddress, DigestInfo> directHitsDigest) 
	{
		Set<DigestInfo> tmp = new HashSet<DigestInfo>();
		for(DigestInfo digestBean: directHitsDigest.values())
		{
			if(!digestBean.isEmpty())
			{
				tmp.add(digestBean);
			}
		}
		int unique = tmp.size();
		int requested = p2pConfiguration.getMinimumResults();
		return new RequestP2PConfiguration(Math.min(unique, requested), p2pConfiguration.getMaxFailure(), p2pConfiguration.getParallelDiff());
	}
}
