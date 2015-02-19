package net.tomp2p.holep.strategy;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.EventExecutorGroup;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.holep.NATType;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.utils.Pair;

public class NonPreservingSequentialStrategy extends AbstractHolePuncher {

	private static final NATType NAT_TYPE = NATType.NON_PRESERVING_SEQUENTIAL;
	private static final Logger LOG = LoggerFactory.getLogger(PortPreservingStrategy.class);

	public NonPreservingSequentialStrategy(final Peer peer, final int numberOfHoles, final int idleUDPSeconds, final Message originalMessage) {
		super(peer, numberOfHoles, idleUDPSeconds, originalMessage);
	}

	@Override
	protected boolean checkReplyValues(Message msg, FutureDone<Message> futureDone) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public FutureDone<Message> replyHolePunch() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected void doPortMappings() {
		// TODO Auto-generated method stub

	}
	
	@Override
	protected SimpleChannelInboundHandler<Message> createAfterHolePHandler(final FutureDone<Message> futureDone) {
		// TODO Auto-generated method stub
		return null;
	}

}
