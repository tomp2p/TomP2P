package net.tomp2p.holep;

import java.util.LinkedList;
import java.util.Queue;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import net.tomp2p.connection.Dispatcher;
import net.tomp2p.holep.strategy.AbstractHolePStrategy;
import net.tomp2p.message.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible that no duplicate messages are processed by the
 * peer. Currently it is used in {@link AbstractHolePStrategy} to filter
 * duplicate messages transmitted by the hole punching procedure.
 * 
 * @author jonaswagner
 * 
 */
@Sharable
public class DuplicatesHandler extends SimpleChannelInboundHandler<Message> {

	private static final int POSITION_ZERO = 0;
	private static final Logger LOG = LoggerFactory.getLogger(DuplicatesHandler.class);
	private final Dispatcher dispatcher;
	private final Thread queueHandlerThread;
	private int messageId = 0;
	private boolean first = true;
	private Queue<Tuple<ChannelHandlerContext, Message>> messageQueue = new LinkedList<Tuple<ChannelHandlerContext, Message>>();
	
	public DuplicatesHandler(final Dispatcher dispatcher) {
		this.dispatcher = dispatcher;
		this.queueHandlerThread = new Thread(new QueueHandler());
	}

	@Override
	protected void channelRead0(final ChannelHandlerContext ctx, final Message msg) throws Exception {
		messageQueue.add(new Tuple<ChannelHandlerContext, Message>(ctx, msg));
		queueHandlerThread.run();
	}

	/**
	 * This method filters all messages out which contain the same messageId as
	 * the first received message with the isExpectDuplicateFlag.
	 */
	private void nextMessage(final ChannelHandlerContext ctx, final Message msg) throws Exception {
		if (msg.isExpectDuplicate()) {
			if (first) {
				first = false;
				messageId = msg.intAt(POSITION_ZERO);
				dispatcher.channelRead(ctx, msg);
				LOG.debug("message with original messageId = " + messageId + " has been received!");
			} else if (messageId == msg.intAt(POSITION_ZERO)) {
				LOG.trace("message with original messageId = " + messageId + " has been ignored!");
			} else {
				LOG.debug("Message received via hole punching will be forwarded to the Dispatcher!");
				dispatcher.channelRead(ctx, msg);
			}
			// if some day UDT or some similar code is integrated into tomp2p,
			// here's where its handler should be placed
		} else {
			LOG.debug("Message received via hole punching will be forwarded to the Dispatcher!");
			dispatcher.channelRead(ctx, msg);
		}
	}

	public class QueueHandler implements Runnable {

		@Override
		public void run() {
			boolean closed = false;
			while (!closed) {
				if (!messageQueue.isEmpty()) {
					Tuple<ChannelHandlerContext, Message> current = messageQueue.poll();
					try {
						nextMessage(current.first(), current.second());
					} catch (Exception e) {
						e.printStackTrace();
						current.first().fireExceptionCaught(e);
					}
				} else {
					closed = true;
				}
			}
		}
	}

	public class Tuple<X, Y> {
		private final X x;
		private final Y y;

		public Tuple(final X x, final Y y) {
			this.x = x;
			this.y = y;
		}

		public X first() {
			return x;
		}

		public Y second() {
			return y;
		}
	}
}
