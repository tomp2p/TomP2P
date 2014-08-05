package net.tomp2p.dht;

import net.tomp2p.connection.PeerBean;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class PeerDHT {

	final private Peer peer;
	final private StorageRPC storageRPC;
	final private DistributedHashTable dht;
	final private StorageLayer storageLayer;

	PeerDHT(Peer peer, StorageLayer storageLayer, DistributedHashTable dht, StorageRPC storageRPC) {
		this.peer = peer;
		this.storageLayer = storageLayer;
		this.dht = dht;
		this.storageRPC = storageRPC;
    }

	public Peer peer() {
		return peer;
	}

	public StorageRPC storeRPC() {
		return storageRPC;
	}

	public DistributedHashTable distributedHashTable() {
		return dht;
	}
	
	public StorageLayer storageLayer() {
		return storageLayer;
	}

	public AddBuilder add(Number160 locationKey) {
		return new AddBuilder(this, locationKey);
	}

	public PutBuilder put(Number160 locationKey) {
		return new PutBuilder(this, locationKey);
	}

	public GetBuilder get(Number160 locationKey) {
		return new GetBuilder(this, locationKey);
	}

	public DigestBuilder digest(Number160 locationKey) {
		return new DigestBuilder(this, locationKey);
	}

	public RemoveBuilder remove(Number160 locationKey) {
		return new RemoveBuilder(this, locationKey);
	}

	/**
	 * The send method works as follows:
	 * 
	 * <pre>
	 * 1. routing: find close peers to the content hash. 
	 *    You can control the routing behavior with 
	 *    setRoutingConfiguration() 
	 * 2. sending: send the data to the n closest peers. 
	 *    N is set via setRequestP2PConfiguration(). 
	 *    If you want to send it to the closest one, use 
	 *    setRequestP2PConfiguration(1, 5, 0)
	 * </pre>
	 * 
	 * @param locationKey
	 *            The target hash to search for during the routing process
	 * @return The send builder that allows to set options
	 */
	public SendBuilder send(Number160 locationKey) {
		return new SendBuilder(this, locationKey);
	}

	public ParallelRequestBuilder<?> parallelRequest(Number160 locationKey) {
		return new ParallelRequestBuilder<FutureDHT<?>>(this, locationKey);
	}

	// ----- convenicence methods ------
	public BaseFuture shutdown() {
	    return peer.shutdown();
    }

	public PeerBean peerBean() {
	    return peer.peerBean();
    }

	public Number160 peerID() {
	    return peer.peerID();
    }

	public PeerAddress peerAddress() {
	    return peer.peerAddress();
    }
}
