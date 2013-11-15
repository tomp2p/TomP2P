package relay;

import java.util.HashMap;
import java.util.Map;

import net.tomp2p.futures.BaseFutureImpl;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.PeerAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import relay.RelayConnections;

public class RelayFuture extends BaseFutureImpl<RelayFuture> {
	
	private final static Logger logger = LoggerFactory.getLogger(RelayFuture.class);
	
	private Map<PeerAddress, String> failedRelays;
	private RelayPeersManager manager;
	
	public RelayFuture() {
		self(this);
		this.failedRelays = new HashMap<PeerAddress, String>();
	}
	
	public void setRelayPeersManager(RelayPeersManager manager) {
		this.manager = manager;
	}
	
	public RelayPeersManager getRelayPeersManager() {
		return manager;
	}

	public boolean done() {
		synchronized (lock) {
			setCompletedAndNotify();
		}
		notifyListeners();
		return true;
	}
}
