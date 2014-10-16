package net.tomp2p.relay.android;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import net.tomp2p.connection.SignatureFactory;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.relay.RelayUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Buffers messages for the unreachable peers. This class is thread-safe.
 * 
 * @author Nico Rutishauser
 *
 */
public class MessageBuffer {

	private static final Logger LOG = LoggerFactory.getLogger(MessageBuffer.class);
	private static final ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor();

	private final int messageCountLimit;
	private final long bufferSizeLimit;
	private final long bufferAgeLimitMS;

	private final AtomicInteger bufferCount;
	private final AtomicLong bufferSize;
	private final MessageBufferListener listener;

	private final ByteBuf buffer;

	private BufferAgeRunnable task;

	public MessageBuffer(int messageCountLimit, long bufferSizeLimit, long bufferAgeLimitMS, MessageBufferListener listener) {
		this.messageCountLimit = messageCountLimit;
		this.bufferSizeLimit = bufferSizeLimit;
		this.bufferAgeLimitMS = bufferAgeLimitMS;
		this.listener = listener;
		this.buffer = Unpooled.buffer();
		this.bufferSize = new AtomicLong();
		this.bufferCount = new AtomicInteger();
	}

	/**
	 * Add a message to the buffer. This method encodes the message first.
	 * 
	 * @throws IOException
	 * @throws SignatureException
	 * @throws InvalidKeyException
	 */
	public void addMessage(Message message, SignatureFactory signatureFactory) throws InvalidKeyException,
			SignatureException, IOException {
		message.restoreContentReferences();
		Buffer encodedMessage = RelayUtils.encodeMessage(message, signatureFactory);
		addMessage(encodedMessage);
		LOG.debug("Added to the buffer: {}", message);
	}

	/**
	 * Add an encoded message to the buffer
	 */
	public void addMessage(Buffer encodedMessage) {
		synchronized (buffer) {
			if (bufferCount.get() == 0) {
				task = new BufferAgeRunnable();
				// schedule the task
				worker.schedule(task, bufferAgeLimitMS, TimeUnit.MILLISECONDS);
			}

			buffer.writeInt(encodedMessage.length());
			buffer.writeBytes(encodedMessage.buffer());
		}

		bufferCount.addAndGet(1);
		bufferSize.addAndGet(encodedMessage.length());
		checkFull();
	}

	private void checkFull() {
		boolean notify = false;
		if (bufferSize.get() >= bufferSizeLimit) {
			LOG.debug("The size of the buffer exceeds the limit of {} bytes", bufferSizeLimit);
			notify = true;
		}

		if (bufferCount.get() >= messageCountLimit) {
			LOG.debug("The number of messages exceeds the maximum message count of {}", messageCountLimit);
			notify = true;
		}

		if (notify) {
			if (task != null) {
				// cancel such that it does not notify the listener twice
				task.cancel();
			}
			notifyAndClear();
		}
	}

	/**
	 * Called when the buffer exceeds either the message count limit. the maximally
	 * allowed buffer size or the maximally allowed age of the first buffer entry. Otherwise
	 * <code>false</code>.
	 */
	private void notifyAndClear() {
		if (bufferCount.get() == 0) {
			LOG.warn("Buffer is empty. Listener won't be notified.");
			return;
		}

		ByteBuf messageBuffer;
		synchronized (buffer) {
			messageBuffer = Unpooled.copiedBuffer(buffer);
			buffer.clear();
			bufferCount.set(0);
			bufferSize.set(0);
		}

		// notify the listener with a copy of the buffer and the segmentation indices
		listener.bufferFull(messageBuffer);
	}

	private class BufferAgeRunnable implements Runnable {

		private final AtomicBoolean cancelled;

		public BufferAgeRunnable() {
			this.cancelled = new AtomicBoolean(false);
		}

		@Override
		public void run() {
			if (!cancelled.get()) {
				LOG.debug("Buffer age exceeds the limit of {}ms", bufferAgeLimitMS);
				notifyAndClear();
			}
		}

		public void cancel() {
			cancelled.set(true);
		}
	}

	/**
	 * Decomposes a buffer containing multiple buffers into an (ordered) list of small buffers. Alternating,
	 * the size of the message and the message itself are encoded in the message buffer. First, the size is
	 * read, then k bytes are read from the buffer (the message). Then again, the size of the next messages is
	 * determined.
	 * 
	 * @param messageBuffer the message buffer
	 * @return a list of buffers
	 */
	public static List<Buffer> decomposeCompositeBuffer(ByteBuf messageBuffer) {
		List<Buffer> buffers = new ArrayList<Buffer>();
		while (messageBuffer.readableBytes() > 0) {
			int size = messageBuffer.readInt();
			ByteBuf message = messageBuffer.readBytes(size);
			buffers.add(new Buffer(message));
		}

		return buffers;
	}
}
