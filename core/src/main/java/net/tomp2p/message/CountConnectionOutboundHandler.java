package net.tomp2p.message;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a soft limit, as there is no way to drop a connection before creating
 * a sockt from Java. This handler counts the incoming connections and drops the
 * connection if a certain limit is reached.
 * 
 * @author Thomas Bocek
 * 
 */
@Sharable
public class CountConnectionOutboundHandler extends ChannelOutboundHandlerAdapter {

	private static final Logger LOG = LoggerFactory.getLogger(CountConnectionOutboundHandler.class);
	private static final AtomicInteger counter = new AtomicInteger();
	final int limit;

	public CountConnectionOutboundHandler(int limit) {
		this.limit = limit;
	}
	
	@Override
	public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
	        ChannelPromise promise) throws Exception {
		int current = -1;
		if ((current = counter.incrementAndGet()) > limit) {
			ctx.channel().close();
			LOG.warn("count warning because: " + current +" > " + limit +" connections active");
		} else {
			ctx.connect(remoteAddress, localAddress, promise);
		}
	}

	@Override
	public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
		counter.decrementAndGet();
	    ctx.close(promise);
	}
}
