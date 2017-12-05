package net.tomp2p.p2p;

import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.rpc.DispatchHandler;

public abstract class AbstractHolePRPC extends DispatchHandler {

	public AbstractHolePRPC(PeerBean peerBean, ConnectionBean connectionBean) {
		super(peerBean, connectionBean);
	}
}
