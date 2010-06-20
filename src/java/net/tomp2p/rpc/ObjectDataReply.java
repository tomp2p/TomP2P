package net.tomp2p.rpc;


import net.tomp2p.peers.PeerAddress;

public interface ObjectDataReply
{
	public Object reply(PeerAddress sender, Object request) throws Exception;
}
