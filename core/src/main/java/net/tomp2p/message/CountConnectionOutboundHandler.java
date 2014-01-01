package net.tomp2p.message;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is a simple counter that counts the current open connections and total connections
 * 
 * @author Thomas Bocek
 * 
 */
@Sharable
public class CountConnectionOutboundHandler extends ChannelOutboundHandlerAdapter {

	private final AtomicInteger counterCurrent = new AtomicInteger();
	private final AtomicInteger counterTotal = new AtomicInteger();
	
	@Override
	public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
	        ChannelPromise promise) throws Exception {
		counterCurrent.incrementAndGet();
		counterTotal.incrementAndGet();
		ctx.connect(remoteAddress, localAddress, promise);
	}

	@Override
	public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
		counterCurrent.decrementAndGet();
	    ctx.close(promise);
	}
	
	public int current() {
		return counterCurrent.get();
	}
	
	public int total() {
		return counterTotal.get();
	}
	
	public void reset() {
		counterCurrent.set(0);
		counterTotal.set(0);
	}
}
