package net.tomp2p.relay.android;

import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.BaseRelayConnection;

public class AndroidRelayConnection extends BaseRelayConnection {

	public AndroidRelayConnection(PeerAddress relayAddress) {
		super(relayAddress);
	}
	
	@Override
	public FutureResponse sendToRelay(Message message) {
		// TODO Auto-generated method stub
		return null;
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

}
