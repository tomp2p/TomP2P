package net.tomp2p.holep;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.connection.Dispatcher;
import net.tomp2p.message.Message;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.ChannelHandler.Sharable;

@Sharable
public class DuplicatesHandler extends SimpleChannelInboundHandler<Message>{

	private static final Logger LOG = LoggerFactory.getLogger(DuplicatesHandler.class);
	private final Dispatcher dispatcher;
	int messageId = 0;
	boolean first = true;
	
	public DuplicatesHandler(Dispatcher dispatcher) {
		this.dispatcher = dispatcher;
	}
	
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
		if (msg.intList() != null && !msg.intList().isEmpty()) {
			if (first) {
				first = false;
				messageId = msg.intAt(0);
				dispatcher.channelRead(ctx, msg);
				LOG.debug("message with original messageId = " + messageId + " has been received!");
			} else {
				LOG.trace("message with original messageId = " + messageId + " has been ignored!");
			}
		} else {
			LOG.debug("Message received via hole punching will be forwarded to the Dispatcher!");
			dispatcher.channelRead(ctx, msg);
		}
	}
}
