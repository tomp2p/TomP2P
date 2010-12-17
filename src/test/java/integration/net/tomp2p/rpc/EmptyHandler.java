package net.tomp2p.rpc;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

/**
 * Dummy handler, only prints exceptions.
 * @author Serge H�nni
 *
 */
@ChannelPipelineCoverage("all")
public class EmptyHandler extends SimpleChannelHandler {
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
		e.getCause().printStackTrace();
	}
}