package net.tomp2p.tracker;

import java.util.Collection;
import java.util.Map;
import java.util.Random;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.TrackerData;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerStatatistic;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Utils;

public class PeerExchange {

	private final Peer peer;
	private final PeerExchangeRPC peerExchangeRPC;
	private final ConnectionConfiguration connectionConfiguration;

	public PeerExchange(final Peer peer, final PeerExchangeRPC peerExchangeRPC,
	        ConnectionConfiguration connectionConfiguration) {
		this.peer = peer;
		this.peerExchangeRPC = peerExchangeRPC;
		this.connectionConfiguration = connectionConfiguration;
	}

	public FutureDone<Void> peerExchange(final PeerAddress remotePeer, final Number320 key, final TrackerData data) {
		return peerExchange(remotePeer, key, data, connectionConfiguration);
	}

	public FutureDone<Void> peerExchange(final PeerAddress remotePeer, final Number320 key, final TrackerData data,
	        final ConnectionConfiguration connectionConfiguration) {
		final FutureDone<Void> futureDone = new FutureDone<Void>();
		FutureChannelCreator futureChannelCreator = peer.connectionBean().reservation().create(1, 0);
		futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
			@Override
			public void operationComplete(FutureChannelCreator future) throws Exception {
				if (future.isSuccess()) {
					final ChannelCreator channelCreator = future.channelCreator();
					FutureResponse futureResponse = peerExchangeRPC.peerExchange(remotePeer, key, channelCreator, data,
					        connectionConfiguration);
					futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
						@Override
						public void operationComplete(FutureResponse future) throws Exception {
							if (future.isSuccess()) {
								futureDone.done();
							} else {
								futureDone.failed(future);
							}
							channelCreator.shutdown();
						}
					});
				} else {
					futureDone.failed(future);
				}
			}
		});
		return futureDone;
	}
	
	public PeerExchangeRPC peerExchangeRPC() {
		return peerExchangeRPC;
	}
	
	
}
