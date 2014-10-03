package net.tomp2p.relay.android;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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

	private final AtomicLong bufferSize;
	private final MessageBufferListener listener;

	private final CompositeByteBuf buffer;
	private final List<Integer> segmentSizes;

	private BufferAgeRunnable task;

	public MessageBuffer(int messageCountLimit, long bufferSizeLimit, long bufferAgeLimitMS, MessageBufferListener listener) {
		this.messageCountLimit = messageCountLimit;
		this.bufferSizeLimit = bufferSizeLimit;
		this.bufferAgeLimitMS = bufferAgeLimitMS;
		this.listener = listener;
		this.buffer = Unpooled.compositeBuffer();
		this.segmentSizes = Collections.synchronizedList(new ArrayList<Integer>());
		this.bufferSize = new AtomicLong();
	}

	/**
	 * Add a message to the buffer
	 */
	public void addMessage(Message message) throws InvalidKeyException, SignatureException, IOException {
		message.restoreContentReferences();
		Buffer encodedMessage = RelayUtils.encodeMessage(message);

		synchronized (segmentSizes) {
			if (buffer.numComponents() == 0) {
				task = new BufferAgeRunnable();
				// schedule the task
				worker.schedule(task, bufferAgeLimitMS, TimeUnit.MILLISECONDS);
			}

			buffer.addComponent(encodedMessage.buffer());
			segmentSizes.add(encodedMessage.length());
		}

		bufferSize.addAndGet(encodedMessage.length());
		LOG.debug("Added to the buffer: {}", message);
		checkFull();
	}

	private void checkFull() {
		boolean notify = false;
		if (bufferSize.get() >= bufferSizeLimit) {
			LOG.debug("The size of the buffer exceeds the limit of {} bytes", bufferSizeLimit);
			notify = true;
		}

		synchronized (segmentSizes) {
			if (segmentSizes.size() >= messageCountLimit) {
				LOG.debug("The number of messages exceeds the maximum message count of {}", messageCountLimit);
				notify = true;
			}
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
		synchronized (segmentSizes) {
			if (segmentSizes.isEmpty()) {
				return;
			}

			ByteBuf messageBuffer = Unpooled.copiedBuffer(buffer.consolidate().array());
			ByteBuf sizeBuffer = Unpooled.buffer();
			for (Integer size : segmentSizes) {
				sizeBuffer.writeInt(size);
			}

			// notify the listener with a copy of the buffer and the segmentation indices
			listener.bufferFull(new Buffer(sizeBuffer), new Buffer(messageBuffer));

			buffer.clear();
			segmentSizes.clear();
			bufferSize.set(0);
		}
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
	 * Decomposes a (large) buffer containing multiple buffers into an (ordered) list of small buffers.
	 * 
	 * @param messageBuffer the large buffer
	 * @param segmentationSizes a list of all sizes of the buffer segments
	 * @return a list of buffers
	 */
	public static List<Buffer> decomposeCompositeBuffer(Buffer sizeBuffer, Buffer messageBuffer) {
		// decode the sizes first
		List<Integer> messageSizes = new ArrayList<Integer>();
		while (sizeBuffer.buffer().readableBytes() >= 4) {
			messageSizes.add(sizeBuffer.buffer().readInt());
		}

		List<Buffer> buffers = new ArrayList<Buffer>();
		int readIndex = 0;
		for (Integer size : messageSizes) {
			buffers.add(new Buffer(messageBuffer.buffer().slice(readIndex, size)));
			readIndex += size;
		}

		return buffers;
	}
}
