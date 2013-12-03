package relay;

import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.futures.FuturePeerConnection;

public class RelayConnections {
	
	private int maxRelays;
	private AtomicInteger connectionCount;
	private AtomicInteger establishedConnectionCount;
	
	public RelayConnections(int maxRelays) {
		this.maxRelays = maxRelays;
		connectionCount = new AtomicInteger();
		establishedConnectionCount = new AtomicInteger();
	}
	
	public AtomicInteger connectionCount() {
		return connectionCount;
	}
	
	public AtomicInteger establishedConnectionCound() {
		return establishedConnectionCount;
	}
	
	public void connectionEstablished() {
		establishedConnectionCount.getAndIncrement();
	}
	
	public void connectionLost() {
		establishedConnectionCount.getAndDecrement();
	}
	
}
