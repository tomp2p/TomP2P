package net.tomp2p.holep;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import net.tomp2p.connection.Dispatcher;
import net.tomp2p.message.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Sharable
public class DuplicatesHandler extends SimpleChannelInboundHandler<Message>{

	private static final int POSITION_ZERO = 0;
	private static final Logger LOG = LoggerFactory.getLogger(DuplicatesHandler.class);
	private final Dispatcher dispatcher;
	int messageId = 0;
	boolean first = true;
	
	public DuplicatesHandler(Dispatcher dispatcher) {
		this.dispatcher = dispatcher;
	}
	
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
		if (msg.isExpectDuplicate()) {
			System.err.println("passed, " + msg.intAt(POSITION_ZERO));
			if (first) {
				first = false;
				System.err.println("passed first");
				messageId = msg.intAt(POSITION_ZERO);
				dispatcher.channelRead(ctx, msg);
				LOG.debug("message with original messageId = " + messageId + " has been received!");
			} else if (messageId == msg.intAt(POSITION_ZERO)){
				LOG.trace("message with original messageId = " + messageId + " has been ignored!");
			} else {
				LOG.debug("Message received via hole punching will be forwarded to the Dispatcher!");
				dispatcher.channelRead(ctx, msg);
			}
		} else {
			LOG.debug("Message received via hole punching will be forwarded to the Dispatcher!");
			dispatcher.channelRead(ctx, msg);
		}
	}
}
