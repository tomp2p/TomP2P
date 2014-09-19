package net.tomp2p.relay.android;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

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

	private final int messageCountLimit;
	private final long bufferSizeLimit;
	private final long bufferAgeLimit;

	private final List<Buffer> buffer;
	private final AtomicLong bufferSize;
	private final AtomicLong firstEntryTime;

	public MessageBuffer(int messageCountLimit, long bufferSizeLimit, long bufferAgeLimit) {
		this.messageCountLimit = messageCountLimit;
		this.bufferSizeLimit = bufferSizeLimit;
		this.bufferAgeLimit = bufferAgeLimit;
		this.buffer = Collections.synchronizedList(new ArrayList<Buffer>());
		this.bufferSize = new AtomicLong();
		this.firstEntryTime = new AtomicLong();
	}

	/**
	 * Add a message to the buffer
	 */
	public void addMessage(Message message) throws InvalidKeyException, SignatureException, IOException {
		Buffer encodedMessage = RelayUtils.encodeMessage(message);

		synchronized (buffer) {
			if(buffer.isEmpty()) {
				// update the time of the first message
				firstEntryTime.set(System.currentTimeMillis());
			}
			
			buffer.add(encodedMessage);
		}

		bufferSize.addAndGet(encodedMessage.length());
	}

	/**
	 * @return <code>true</code> when the buffer exceeds either the message count limit. the maximally
	 *         allowed buffer size or the maximally allowed age of the first buffer entry. Otherwise <code>false</code>.
	 */
	public boolean isFull() {
		if (bufferSize.get() >= bufferSizeLimit) {
			// the size of the buffer content exceeds the limit
			return true;
		}

		synchronized (buffer) {
			if(buffer.isEmpty()) {
				return false;
			} else if(buffer.size() >= messageCountLimit){
				// the number of messages in the buffer exceeds the limit
				return true;
			} else {
				// check if the age exceeds
				return firstEntryTime.get() + bufferAgeLimit >= System.currentTimeMillis();
			}
		}
	}
	
	/**
	 * Returns the buffer while also cleaning it
	 * @return
	 */
	public List<Buffer> getAndClear() {
		synchronized (buffer) {
			List<Buffer> copy = new ArrayList<Buffer>(buffer);
			buffer.clear();
			bufferSize.set(0);
			return copy;
		}
	}
}
