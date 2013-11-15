package relay;

import java.util.concurrent.atomic.AtomicInteger;

public class RelayConnections {
	
	private int maxRelays;
	private AtomicInteger connectionCount;
	
	public RelayConnections(int maxRelays) {
		this.maxRelays = maxRelays;
		connectionCount = new AtomicInteger();
	}
	
	public AtomicInteger connectionCount() {
		return connectionCount;
	}
	
}
