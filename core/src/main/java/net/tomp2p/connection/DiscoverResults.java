package net.tomp2p.connection;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;

public class DiscoverResults {

	private final Collection<InetAddress> newAddresses;
	private final Collection<InetAddress> newBroadcastAddresses;
	private final Collection<InetAddress> removedFoundAddresses;
	private final Collection<InetAddress> removedFoundBroadcastAddresses;
	private final Collection<InetAddress> existingAddresses;
	private final Collection<InetAddress> existingBroadcastAddresses;
	private final String status;
	private final boolean listenAny;

	public DiscoverResults(Collection<InetAddress> newAddresses, Collection<InetAddress> newBroadcastAddresses,
	        Collection<InetAddress> removedFoundAddresses, Collection<InetAddress> removedFoundBroadcastAddresses,
	        Collection<InetAddress> existingAddresses, Collection<InetAddress> existingBroadcastAddresses, boolean listenAny, String status) {
		this.newAddresses = newAddresses;
		this.newBroadcastAddresses = newBroadcastAddresses;
		this.removedFoundAddresses = removedFoundAddresses;
		this.removedFoundBroadcastAddresses = removedFoundBroadcastAddresses;
		this.existingAddresses = existingAddresses;
		this.existingBroadcastAddresses = existingBroadcastAddresses;
		this.listenAny = listenAny;
		this.status = status;
	}

	public Collection<InetAddress> newAddresses() {
		return newAddresses;
	}

	public Collection<InetAddress> newBroadcastAddresses() {
		return newBroadcastAddresses;
	}

	public Collection<InetAddress> removedFoundAddresses() {
		return removedFoundAddresses;
	}

	public Collection<InetAddress> removedFoundBroadcastAddresses() {
		return removedFoundBroadcastAddresses;
	}
	
	public Collection<InetAddress> existingAddresses() {
		return existingAddresses;
	}

	public Collection<InetAddress> existingBroadcastAddresses() {
		return existingBroadcastAddresses;
	}

	public boolean isListenAny() {
		return listenAny;
	}
	
	public String status() {
		return status;
	}

	public boolean isEmpty() {
	    return newAddresses.isEmpty() && newBroadcastAddresses.isEmpty() 
	    		&& removedFoundAddresses.isEmpty() && removedFoundBroadcastAddresses.isEmpty();
    }

	public InetAddress foundAddress() {
		InetAddress localhost = null;

		// first check for IPv4
		for (InetAddress inetAddress : newAddresses) {
			if (inetAddress instanceof Inet4Address) {
				if(inetAddress.isLoopbackAddress()) {
					// don't prefer loopback addresses over other ones
					localhost = inetAddress;
				} else {
					return inetAddress;
				}
			}
		}
		
		// TODO prefer local IPv4 over other addresses?
		if(localhost != null) {
			return localhost;
		}
		
		if (!newAddresses.isEmpty()) {
			return newAddresses.iterator().next();
		}
		return null;
    }
	
	public SocketAddress wildCardSocket() {
		InetAddress foundAddress = foundAddress();
		if(foundAddress != null) {
			 return new InetSocketAddress(foundAddress, 0);
		} else {
			return new InetSocketAddress(0);
		}
    }

}
