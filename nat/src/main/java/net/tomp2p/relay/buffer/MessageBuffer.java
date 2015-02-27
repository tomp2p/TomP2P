package net.tomp2p.relay.buffer;

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

/**
 * Buffers messages for the unreachable peers. This class is thread-safe.
 * If the buffer is full, the {@link MessageBufferListener}s are triggered. In the mean time, another list
 * holds the previously buffered messages, until the buffer is collected.
 * 
 * @author Nico Rutishauser
 *
 */
public class MessageBuffer<T> {

	private static final Logger LOG = LoggerFactory.getLogger(MessageBuffer.class);
	private static final ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor();

	private final int messageCountLimit;
	private final long bufferSizeLimit;
	private final long bufferAgeLimitMS;

	private final AtomicLong bufferSize;
	private final List<MessageBufferListener<T>> listeners;

	private final List<T> buffer;

	private BufferAgeRunnable task;

	/**
	 * Create a new buffer using the configuration
	 * 
	 * @param config the buffer limit configuration
	 */
	public MessageBuffer(MessageBufferConfiguration config) {
		this(config.bufferCountLimit(), config.bufferSizeLimit(), config.bufferAgeLimit());
	}

	/**
	 * Create a new buffer with given limits
	 * 
	 * @param bufferCountLimit the number of messages
	 * @param bufferSizeLimit the size of all messages (in bytes)
	 * @param bufferAgeLimitMS the maximum age of the oldest message
	 */
	public MessageBuffer(int bufferCountLimit, long bufferSizeLimit, long bufferAgeLimitMS) {
		this.messageCountLimit = bufferCountLimit;
		this.bufferSizeLimit = bufferSizeLimit;
		this.bufferAgeLimitMS = bufferAgeLimitMS;
		this.listeners = new ArrayList<MessageBufferListener<T>>();
		this.buffer = Collections.synchronizedList(new ArrayList<T>());
		this.bufferSize = new AtomicLong();
	}

	public void addListener(MessageBufferListener<T> listener) {
		listeners.add(listener);
	}

	/**
	 * Add an encoded message to the buffer
	 * 
	 * @throws IOException
	 * @throws SignatureException
	 * @throws InvalidKeyException
	 */
	public void addMessage(T message, long messageSize) {
		synchronized (buffer) {
			if (buffer.isEmpty()) {
				task = new BufferAgeRunnable();
				// schedule the task
				worker.schedule(task, bufferAgeLimitMS, TimeUnit.MILLISECONDS);
			}

			buffer.add(message);
		}

		bufferSize.addAndGet(messageSize);

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
			notifyAndClear(true);
		}
	}

	/**
	 * Flush the buffer and notify the listeners
	 */
	public void flushNow() {
		// no need to flush the buffer because it's empty
		synchronized (buffer) {
			if(buffer.isEmpty()) {
				return;
			}
		}
		
		LOG.trace("Flushing buffer...");
		if (task != null) {
			// cancel such that it does not notify the listener twice
			task.cancel();
		}
		notifyAndClear(false);
	}

	/**
	 * Called when the buffer exceeds either the message count limit. the maximally
	 * allowed buffer size or the maximally allowed age of the first buffer entry. Otherwise
	 * <code>false</code>.
	 * 
	 * @param wasFull <code>true</code> if this method was triggered because of buffer overflow.
	 *            <code>False</code> if this method was triggered manually (because messages need to be ready now.
	 */
	private void notifyAndClear(boolean wasFull) {
		List<T> copy;
		synchronized (buffer) {
			if (buffer.isEmpty()) {
				LOG.warn("Buffer is empty. Listener won't be notified.");
				return;
			}

			copy = new ArrayList<T>(buffer);
			buffer.clear();
			bufferSize.set(0);
		}

		// notify the listeners with a copy of the buffer and the segmentation indices
		for (MessageBufferListener<T> listener : listeners) {
			if(wasFull) {
				listener.bufferFull(copy);
			} else {
				listener.bufferFlushed(copy);
			}
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
				notifyAndClear(true);
			}
		}

		public void cancel() {
			cancelled.set(true);
		}
	}
}
