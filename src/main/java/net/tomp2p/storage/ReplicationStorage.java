package net.tomp2p.storage;

import java.util.Collection;

import net.tomp2p.peers.Number160;

public interface ReplicationStorage
{
	public abstract Number160 findPeerIDForResponsibleContent(Number160 locationKey);
	
	public abstract Collection<Number160> findContentForResponsiblePeerID(Number160 peerID);
	
	public boolean updateResponsibilities(Number160 locationKey, Number160 peerId);
	
	public void removeResponsibility(Number160 locationKey);

}
