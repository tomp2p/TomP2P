package net.tomp2p.dht;

import net.tomp2p.p2p.Peer;
import net.tomp2p.storage.Storage;

public class PeerBuilderDHT {

	final private Peer peer;
	private StorageRPC storageRPC;
	private DistributedHashTable dht;
	private StorageLayer storageLayer;
	private Storage storage;

	public PeerBuilderDHT(Peer peer) {
		this.peer = peer;
	}

	public Peer peer() {
		return peer;
	}

	public StorageRPC storeRPC() {
		return storageRPC;
	}

	public PeerBuilderDHT storeRPC(StorageRPC storeRPC) {
		this.storageRPC = storeRPC;
		return this;
	}

	public DistributedHashTable distributedHashTable() {
		return dht;
	}

	public PeerBuilderDHT distributedHashTable(DistributedHashTable dht) {
		this.dht = dht;
		return this;
	}

	public StorageLayer storageLayer() {
		return storageLayer;
	}

	public PeerBuilderDHT storageLayer(StorageLayer storageLayer) {
		this.storageLayer = storageLayer;
		return this;
	}

	public Storage storage() {
		return storage;
	}

	public PeerBuilderDHT storage(Storage storage) {
		this.storage = storage;
		return this;

	}

	public PeerDHT start() {
		if (storage == null) {
			storage = new StorageMemory();
		}
		if (storageLayer == null) {
			storageLayer = new StorageLayer(storage);
			storageLayer.start(peer.connectionBean().timer(), storageLayer.storageCheckIntervalMillis());
		}
		if (peer.peerBean().digestStorage() == null) {
			peer.peerBean().digestStorage(storageLayer);
		}
		if (storageRPC == null) {
			storageRPC = new StorageRPC(peer.peerBean(), peer.connectionBean(), storageLayer);
		}
		if (dht == null) {
			dht = new DistributedHashTable(peer.distributedRouting(), storageRPC, peer.directDataRPC());
		}

		return new PeerDHT(peer, storageLayer, dht, storageRPC);
	}
}
