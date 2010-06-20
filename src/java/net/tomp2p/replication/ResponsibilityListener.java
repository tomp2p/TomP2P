package net.tomp2p.replication;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public interface ResponsibilityListener
{

	public void meResponsible(Number160 locationKey);

	public void otherResponsible(Number160 locationKey, PeerAddress other);
}
