package net.tomp2p.relay.android;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.List;

import net.tomp2p.connection.DSASignatureFactory;
import net.tomp2p.connection.SignatureFactory;
import net.tomp2p.message.Message;
import net.tomp2p.relay.UtilsNAT;

import org.junit.Test;

public class TestMessageBuffer {

	private final SignatureFactory signature = new DSASignatureFactory();

	@Test
	public void testReachCountLimit() throws InvalidKeyException, SignatureException, IOException {
		CountingBufferListener listener = new CountingBufferListener();
		MessageBuffer buffer = new MessageBuffer(3, Long.MAX_VALUE, Long.MAX_VALUE);
		buffer.addListener(listener);

		// create three messages
		Message first = UtilsNAT.createRandomMessage();
		Message second = UtilsNAT.createRandomMessage();
		Message third = UtilsNAT.createRandomMessage();

		buffer.addMessage(first, signature);
		buffer.addMessage(second, signature);

		// buffer did not trigger yet
		assertEquals(0, listener.getTriggerCount());
		assertEquals(0, listener.getBuffer().size());

		// buffer triggered now
		buffer.addMessage(third, signature);
		assertEquals(1, listener.getTriggerCount());
		assertEquals(3, listener.getBuffer().size());
	}

	@Test
	public void testReachSizeLimit() throws InvalidKeyException, SignatureException, IOException {
		CountingBufferListener listener = new CountingBufferListener();
		MessageBuffer buffer = new MessageBuffer(Integer.MAX_VALUE, 1, Long.MAX_VALUE);
		buffer.addListener(listener);

		// create one message
		buffer.addMessage(UtilsNAT.createRandomMessage(), signature);

		// buffer triggered already
		assertEquals(1, listener.getTriggerCount());
		assertEquals(1, listener.getBuffer().size());
	}

	@Test
	public void testReachAgeLimit() throws InvalidKeyException, SignatureException, IOException, InterruptedException {
		long waitTime = 2000;

		CountingBufferListener listener = new CountingBufferListener();
		MessageBuffer buffer = new MessageBuffer(Integer.MAX_VALUE, Long.MAX_VALUE, waitTime);
		buffer.addListener(listener);

		// create one message
		buffer.addMessage(UtilsNAT.createRandomMessage(), signature);

		// buffer did not trigger yet
		assertEquals(0, listener.getTriggerCount());
		assertEquals(0, listener.getBuffer().size());

		// wait for the given time and slightly longer
		Thread.sleep((long) (waitTime * 1.5));

		// buffer triggered already
		assertEquals(1, listener.getTriggerCount());
		assertEquals(1, listener.getBuffer().size());

		// wait again
		Thread.sleep((long) (waitTime * 1.5));

		// test that buffer did not trigger again
		assertEquals(1, listener.getTriggerCount());
		assertEquals(1, listener.getBuffer().size());
	}

	@Test
	public void testBufferOrder() throws InvalidKeyException, SignatureException, IOException, NoSuchAlgorithmException, InvalidKeySpecException {
		CountingBufferListener listener = new CountingBufferListener();
		MessageBuffer buffer = new MessageBuffer(5, Long.MAX_VALUE, Long.MAX_VALUE);
		buffer.addListener(listener);

		// create five messages
		Message first = UtilsNAT.createRandomMessage();
		Message second = UtilsNAT.createRandomMessage();
		Message third = UtilsNAT.createRandomMessage();
		Message fourth = UtilsNAT.createRandomMessage();
		Message fifth = UtilsNAT.createRandomMessage();

		buffer.addMessage(first, signature);
		buffer.addMessage(second, signature);
		buffer.addMessage(third, signature);
		buffer.addMessage(fourth, signature);
		buffer.addMessage(fifth, signature);
		
		// buffer triggered by now, check the order
		List<Message> content = listener.getBuffer();
		assertEquals(first.messageId(), content.get(0).messageId());
		assertEquals(second.messageId(),content.get(1).messageId());
		assertEquals(third.messageId(), content.get(2).messageId());
		assertEquals(fourth.messageId(), content.get(3).messageId());
		assertEquals(fifth.messageId(), content.get(4).messageId());
	}
	
	@Test
	public void testGarbageCollect() throws InvalidKeyException, SignatureException, IOException, InterruptedException {
		CountingBufferListener listener = new CountingBufferListener();
		MessageBuffer buffer = new MessageBuffer(2, Long.MAX_VALUE, Long.MAX_VALUE);
		buffer.addListener(listener);

		// create one message
		buffer.addMessage(UtilsNAT.createRandomMessage(), signature);

		// garbage collect
		System.gc();

		// create another message
		buffer.addMessage(UtilsNAT.createRandomMessage(), signature);
				
		// buffer triggered two messages
		assertEquals(2, listener.getBuffer().size());
	}
	
	private class CountingBufferListener implements MessageBufferListener {

		private final List<Message> buffer;
		private int bufferFullTriggerCount;

		public CountingBufferListener() {
			this.buffer = new ArrayList<Message>();
			this.bufferFullTriggerCount = 0;
		}

		@Override
		public void bufferFull(List<Message> messageBuffer) {
			// instantly decompose since we don't need to send it here
			this.buffer.addAll(messageBuffer);
			bufferFullTriggerCount++;
		}

		public int getTriggerCount() {
			return bufferFullTriggerCount;
		}

		public List<Message> getBuffer() {
			return buffer;
		}
	}
}
