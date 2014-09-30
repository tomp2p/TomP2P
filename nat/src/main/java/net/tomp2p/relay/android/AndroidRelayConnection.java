package net.tomp2p.relay.android;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.BaseRelayConnection;
import net.tomp2p.relay.RelayUtils;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC.Commands;

public class AndroidRelayConnection extends BaseRelayConnection {

	private final DispatchHandler dispatchHandler;
	private final Peer peer;
	private final ConnectionConfiguration config;
	private final GCMServerCredentials gcmServerCredentials;

	public AndroidRelayConnection(PeerAddress relayAddress, DispatchHandler dispatchHandler, Peer peer, ConnectionConfiguration config, GCMServerCredentials gcmServerCredentials) {
		super(relayAddress);
		this.dispatchHandler = dispatchHandler;
		this.peer = peer;
		this.config = config;
		this.gcmServerCredentials = gcmServerCredentials;
		
	}
	
	@Override
	public FutureResponse sendToRelay(Message message) {
		// send it over a newly opened connection
    	return RelayUtils.connectAndSend(peer, message, config);
	}
	
	public FutureResponse sendBufferRequest() {
		Message message = dispatchHandler.createMessage(relayAddress(), Commands.RELAY.getNr(), Type.REQUEST_4);
		// close the connection after this message
		message.keepAlive(false);
		return sendToRelay(message);
	}

	@Override
	public FutureDone<Void> shutdown() {
		// TODO Auto-generated method stub
		return new FutureDone<Void>().done();
	}

	@Override
	public void onMapUpdateFailed() {
		// TODO Auto-generated method stub
	}

	@Override
	public void onMapUpdateSuccess() {
		// TODO Auto-generated method stub
	}

	public GCMServerCredentials gcmServerCredentials() {
		return gcmServerCredentials;
	}

}
