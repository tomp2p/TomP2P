package net.tomp2p.relay.android;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.relay.RelayUtils;

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

	private final List<Buffer> buffer;
	private final AtomicLong bufferSize;
	private final BufferFullListener listener;

	private BufferAgeRunnable task;

	public MessageBuffer(int messageCountLimit, long bufferSizeLimit, long bufferAgeLimitMS, BufferFullListener listener) {
		this.messageCountLimit = messageCountLimit;
		this.bufferSizeLimit = bufferSizeLimit;
		this.bufferAgeLimitMS = bufferAgeLimitMS;
		this.listener = listener;
		this.buffer = Collections.synchronizedList(new ArrayList<Buffer>());
		this.bufferSize = new AtomicLong();
	}

	/**
	 * Add a message to the buffer
	 */
	public void addMessage(Message message) throws InvalidKeyException, SignatureException, IOException {
		message.restoreContentReferences();
		Buffer encodedMessage = RelayUtils.encodeMessage(message);

		synchronized (buffer) {
			if (buffer.isEmpty()) {
				task = new BufferAgeRunnable();
				// schedule the task
				worker.schedule(task, bufferAgeLimitMS, TimeUnit.MILLISECONDS);
			}

			buffer.add(encodedMessage);
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

		synchronized (buffer) {
			if (buffer.size() >= messageCountLimit) {
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
		List<Buffer> copy = new ArrayList<Buffer>();
		synchronized (buffer) {
			copy.addAll(buffer);
			buffer.clear();
			bufferSize.set(0);
		}

		if (!copy.isEmpty() && listener != null) {
			listener.bufferFull(copy);
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
}
