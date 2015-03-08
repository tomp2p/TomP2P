package net.tomp2p.connection;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a soft limit, as there is no way to drop a connection before creating
 * a socket from Java. This handler counts the incoming connections and drops the
 * connection if a certain limit is reached.
 * 
 * @author Thomas Bocek
 * 
 */
@Sharable
public class DropConnectionInboundHandler extends ChannelInboundHandlerAdapter {

	private static final Logger LOG = LoggerFactory.getLogger(DropConnectionInboundHandler.class);
	private final AtomicInteger counter = new AtomicInteger();
	final int limit;

	public DropConnectionInboundHandler(int limit) {
		this.limit = limit;
	}

	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		int current;
		if ((current = counter.incrementAndGet()) > limit) {
			ctx.channel().close();
			LOG.warn("Dropped connection because {} > {} connections active.",  current, limit);
		}
		ctx.fireChannelRegistered();
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		counter.decrementAndGet();
		ctx.fireChannelInactive();
	}
}
