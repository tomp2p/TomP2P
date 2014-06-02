package net.tomp2p.replication;

import java.util.Random;

import net.tomp2p.dht.PeerDHT;
import net.tomp2p.dht.ReplicationListener;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.Replication;
import net.tomp2p.p2p.ReplicationExecutor;
import net.tomp2p.p2p.StorageLayer;

public class PeerReplication implements ReplicationListener {
	
	private ReplicationExecutor replicationExecutor = null;
	
	private ReplicationFactor replicationFactor = null;

	private ReplicationSender replicationSender = null;

	public PeerReplication(PeerDHT peerDHT) {
		
		peerDHT.storeRPC().replicationListener(this);
	 
		if (random == null) {
			random = new Random();
		}
		if (intervalMillis == -1) {
			intervalMillis = 60 * 1000;
		}
		if (delayMillis == -1) {
			delayMillis = 30 * 1000;
		}

		if (replicationFactor == null) {
			replicationFactor = new ReplicationFactor() {
				@Override
				public int replicationFactor() {
					// Default is 6 as in the builders
					return 6;
				}

				@Override
                public void init(Peer peer) {
                }
			};
		}
		//here we need the peermap to be ready
		replicationFactor.init(peer);

		if (replicationSender == null) {
			replicationSender = new ReplicationExecutor.DefaultReplicationSender();
		}
		replicationSender.init(peer);

		// indirect replication
		if (replicationExecutor == null && isEnableIndirectReplication() && isEnableStorageRPC()) {
			replicationExecutor = new ReplicationExecutor(peer, replicationFactor, replicationSender, random,
			        connectionBean.timer(), delayMillis, allPeersReplicate);
		}
		if (replicationExecutor != null) {
			replicationExecutor.init(intervalMillis);
		}
		peerBean.replicationExecutor(replicationExecutor);
		
		
		
		
		// peerBean.setStorage(getStorage());
				Replication replicationStorage = new Replication(new StorageLayer(storage), peerBean.serverPeerAddress(), peerMap, 5,
						enableNRootReplication);
				peerBean.replicationStorage(replicationStorage);
		
    }
	
	public ReplicationExecutor replicationExecutor() {
		return replicationExecutor;
	}

	public PeerBuilder replicationExecutor(ReplicationExecutor replicationExecutor) {
		this.replicationExecutor = replicationExecutor;
		return this;
	}
	
	public ReplicationFactor replicationFactor() {
		return replicationFactor;
	}

	public PeerBuilder replicationFactor(ReplicationFactor replicationFactor) {
		this.replicationFactor = replicationFactor;
		return this;
	}

	public ReplicationSender replicationSender() {
		return replicationSender;
	}

	public PeerBuilder replicationSender(ReplicationSender replicationSender) {
		this.replicationSender = replicationSender;
		return this;
	}
	
}
