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
package net.tomp2p.tracker;

import java.security.PublicKey;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.RequestHandler;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.TrackerData;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerStatistic;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrackerRPC extends DispatchHandler {
	private static final Logger LOG = LoggerFactory.getLogger(TrackerRPC.class);

	public static final int MAX_MSG_SIZE_UDP = 35;

	final private TrackerStorage trackerStorage;

	/**
	 * @param peerBean
	 * @param connectionBean
	 */
	public TrackerRPC(final PeerBean peerBean, final ConnectionBean connectionBean, TrackerStorage trackerStorage) {
		super(peerBean, connectionBean);
		register(RPC.Commands.TRACKER_ADD.getNr(), RPC.Commands.TRACKER_GET.getNr());
		this.trackerStorage = trackerStorage;
	}

	public FutureResponse addToTracker(final PeerAddress remotePeer, AddTrackerBuilder builder,
	        ChannelCreator channelCreator) {

		Utils.nullCheck(remotePeer, builder.locationKey(), builder.domainKey());
		final Message message = createMessage(remotePeer, RPC.Commands.TRACKER_ADD.getNr(), Type.REQUEST_3);
		if (builder.isSign()) {
			message.publicKeyAndSign(builder.keyPair());
		}
		message.key(builder.locationKey());
		message.key(builder.domainKey());
		if (builder.getBloomFilter() != null) {
			message.bloomFilter(builder.getBloomFilter());
		}
		final FutureResponse futureResponse = new FutureResponse(message);

		addTrackerDataListener(futureResponse, new Number320(builder.locationKey(), builder.domainKey()), message);
		RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(futureResponse, peerBean(),
		        connectionBean(), builder);

		TrackerData trackerData = new TrackerData(new HashMap<PeerAddress, Data>());
		PeerAddress peerAddressToAnnounce = builder.peerAddressToAnnounce();
		if (peerAddressToAnnounce == null) {
			peerAddressToAnnounce = peerBean().serverPeerAddress();
		}
		trackerData.put(peerAddressToAnnounce, builder.attachement());
		trackerData = UtilsTracker.limit(trackerData, TrackerRPC.MAX_MSG_SIZE_UDP);
		message.trackerData(trackerData);

		LOG.debug("tracker PUT {}", message);
		if (builder.isForceTCP() || builder.attachement()!=null) {
			return requestHandler.sendTCP(channelCreator);
		} else {
			return requestHandler.sendUDP(channelCreator);
		}
	}

	public FutureResponse getFromTracker(final PeerAddress remotePeer, GetTrackerBuilder builder,
	        ChannelCreator channelCreator) {

		Utils.nullCheck(remotePeer, builder.locationKey(), builder.domainKey());
		final Message message = createMessage(remotePeer, RPC.Commands.TRACKER_GET.getNr(), Type.REQUEST_1);
		
		if (builder.isSign()) {
			message.publicKeyAndSign(builder.keyPair());
		}
		message.key(builder.locationKey());
		message.key(builder.domainKey());
		// TODO: make this always a bloom filter
		if (builder.knownPeers() != null && (builder.knownPeers() instanceof SimpleBloomFilter)) {
			message.bloomFilter((SimpleBloomFilter<Number160>) builder.knownPeers());
		}

		FutureResponse futureResponse = new FutureResponse(message);
		addTrackerDataListener(futureResponse, new Number320(builder.locationKey(), builder.domainKey()), message);

		RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(futureResponse, peerBean(),
		        connectionBean(), builder);

		LOG.debug("tracker GET {}", message);
		if ((builder.isExpectAttachement() || builder.isForceTCP())) {
			return requestHandler.sendTCP(channelCreator);
		} else {
			return requestHandler.sendUDP(channelCreator);
		}
	}

	private void addTrackerDataListener(FutureResponse futureResponse, final Number320 key, final Message message) {
		futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
			@Override
			public void operationComplete(FutureResponse future) throws Exception {
				if (future.isSuccess()) {
					// Since I might become a tracker as well, we keep this
					// information
					// about those trackers.
					Message message = future.responseMessage();
					TrackerData tmp = message.trackerData(0);
					// no data found
					if (tmp == null || tmp.size() == 0) {
						return;
					}

					for (Map.Entry<PeerAddress, Data> trackerData : tmp.peerAddresses().entrySet()) {
						// we don't know the public key, since this is not first
						// hand information. TTL will be set in tracker storage,
						// so don't worry about it here.
						trackerStorage.put(key, trackerData.getKey(), null, trackerData.getValue());
					}
				} else {
					LOG.warn("add tracker failed: msg = {}, {}", message, future.failedReason());
				}
			}
		});

	}

	@Override
	public void handleResponse(Message message, PeerConnection peerConnection, boolean sign, Responder responder)
	        throws Exception {
		LOG.debug("handleResponse on {}", message);
		if (!((message.type() == Type.REQUEST_1 || message.type() == Type.REQUEST_3) && message.key(0) != null && message
		        .key(1) != null)) {
			throw new IllegalArgumentException("Message content is wrong");
		}
		final Message responseMessage = createResponseMessage(message, Type.OK);

		// get data
		Number160 locationKey = message.key(0);
		Number160 domainKey = message.key(1);
		SimpleBloomFilter<Number160> knownPeers = message.bloomFilter(0);

		PublicKey publicKey = message.publicKey(0);
		//
		Collection<Pair<PeerStatistic, Data>> value = trackerStorage.peers(new Number320(locationKey, domainKey)).values(); 
		TrackerData meshPeers = new TrackerData(value);
		
		LOG.debug("found peers on tracker: {}", meshPeers == null ? "null " : meshPeers.peerAddresses());

		boolean couldProvideMoreData = false;
		if (meshPeers != null) {
			if (knownPeers != null) {
				meshPeers = UtilsTracker.disjunction(meshPeers, knownPeers);
			}
			int size = meshPeers.size();

			meshPeers = UtilsTracker.limit(meshPeers, TrackerRPC.MAX_MSG_SIZE_UDP);
			couldProvideMoreData = size > meshPeers.size();
			responseMessage.trackerData(meshPeers);
		}

		if (couldProvideMoreData) {
			responseMessage.type(Message.Type.PARTIALLY_OK);
		}

		if (message.command() == RPC.Commands.TRACKER_ADD.getNr()) {
			TrackerData trackerData = message.trackerData(0);
			if (trackerData.size() != 1) {
				responseMessage.type(Message.Type.EXCEPTION);
			} else {
				Map.Entry<PeerAddress, Data> entry = trackerData.peerAddresses().entrySet().iterator().next();
				if (!trackerStorage.put(new Number320(locationKey, domainKey), entry.getKey(), publicKey,
				        entry.getValue())) {
					responseMessage.type(Message.Type.DENIED);
					LOG.debug("tracker NOT put on({}) locationKey:{}, domainKey:{}, address:{}", peerBean()
					        .serverPeerAddress(), locationKey, domainKey, entry.getKey());
				} else {
					LOG.debug("tracker put on({}) locationKey:{}, domainKey:{}, address: {}", peerBean()
					        .serverPeerAddress(), locationKey, domainKey, entry.getKey());
				}
			}

		} else {
			LOG.debug("tracker get on({}) locationKey:{}, domainKey:{}, address:{}, returning: {}", peerBean()
			        .serverPeerAddress(), locationKey, domainKey, message.sender(), (meshPeers == null ? "0"
			        : meshPeers.size()));
		}
		if (sign) {
			responseMessage.publicKeyAndSign(peerBean().getKeyPair());
		}
		responder.response(responseMessage);
	}
}
