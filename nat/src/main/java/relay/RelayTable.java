package relay;

import io.netty.channel.ChannelHandlerContext;

import java.util.HashMap;
import java.util.Map;

import net.tomp2p.connection2.ConnectionBean;
import net.tomp2p.connection2.PeerBean;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

/**
 * Ignoore this... wrong approach
 * @author rvoellmy
 *
 */
public class RelayTable {
	
	private Map<PeerAddress, ChannelHandlerContext> channelHandlers;
	private Peer peer;
	
	public RelayTable(Peer peer) {
		channelHandlers = new HashMap<PeerAddress, ChannelHandlerContext>();
		this.peer = peer;
	}
	
	public void forwardMessage(PeerAddress pa, Message msg) {
		System.err.println("FORWARDING MESSAGE");
		channelHandlers.get(pa).channel().writeAndFlush(msg);
	}
	
	public void addUnreachablePeer(PeerAddress pa, ChannelHandlerContext ctx) {
		channelHandlers.put(pa, ctx);
		//new RelayForwarder(peer.getConnectionBean(), peer.getPeerBean(), this, pa);
	}

	
}
