package net.tomp2p.relay.android;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.List;
import java.util.Random;

import net.tomp2p.connection.DSASignatureFactory;
import net.tomp2p.connection.SignatureFactory;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.RPC.Commands;

import org.junit.Test;

public class TestMessageBuffer {

	private final SignatureFactory signature = new DSASignatureFactory();

	@Test
	public void testReachCountLimit() throws InvalidKeyException, SignatureException, IOException {
		CountingBufferListener listener = new CountingBufferListener();
		MessageBuffer buffer = new MessageBuffer(3, Long.MAX_VALUE, Long.MAX_VALUE);
		buffer.addListener(listener);

		// create three messages
		Message first = createMessage();
		Message second = createMessage();
		Message third = createMessage();

		buffer.addMessage(first, signature);
		buffer.addMessage(second, signature);

		// buffer did not trigger yet
		assertEquals(0, listener.getTriggerCount());

		// buffer triggered now
		buffer.addMessage(third, signature);
		assertEquals(1, listener.getTriggerCount());
		assertEquals(3, buffer.collectBuffer().size());
	}

	@Test
	public void testReachSizeLimit() throws InvalidKeyException, SignatureException, IOException {
		CountingBufferListener listener = new CountingBufferListener();
		MessageBuffer buffer = new MessageBuffer(Integer.MAX_VALUE, 1, Long.MAX_VALUE);
		buffer.addListener(listener);

		// create one message
		buffer.addMessage(createMessage(), signature);

		// buffer triggered already
		assertEquals(1, listener.getTriggerCount());
		assertEquals(1, buffer.collectBuffer().size());
	}

	@Test
	public void testReachAgeLimit() throws InvalidKeyException, SignatureException, IOException, InterruptedException {
		long waitTime = 2000;

		CountingBufferListener listener = new CountingBufferListener();
		MessageBuffer buffer = new MessageBuffer(Integer.MAX_VALUE, Long.MAX_VALUE, waitTime);
		buffer.addListener(listener);

		// create one message
		buffer.addMessage(createMessage(), signature);

		// buffer did not trigger yet
		assertEquals(0, listener.getTriggerCount());

		// wait for the given time and slightly longer
		Thread.sleep((long) (waitTime * 1.5));

		// buffer triggered already
		assertEquals(1, listener.getTriggerCount());
		assertEquals(1, buffer.collectBuffer().size());

		// wait again
		Thread.sleep((long) (waitTime * 1.5));

		// test that buffer did not trigger again
		assertEquals(1, listener.getTriggerCount());
		assertEquals(0, buffer.collectBuffer().size());
	}

	@Test
	public void testBufferOrder() throws InvalidKeyException, SignatureException, IOException, NoSuchAlgorithmException, InvalidKeySpecException {
		CountingBufferListener listener = new CountingBufferListener();
		MessageBuffer buffer = new MessageBuffer(5, Long.MAX_VALUE, Long.MAX_VALUE);
		buffer.addListener(listener);

		// create five messages
		Message first = createMessage();
		Message second = createMessage();
		Message third = createMessage();
		Message fourth = createMessage();
		Message fifth = createMessage();

		buffer.addMessage(first, signature);
		buffer.addMessage(second, signature);
		buffer.addMessage(third, signature);
		buffer.addMessage(fourth, signature);
		buffer.addMessage(fifth, signature);
		
		// buffer triggered by now, check the order
		List<Message> content = buffer.collectBuffer();
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
		buffer.addMessage(createMessage(), signature);

		// garbage collect
		System.gc();

		// create another message
		buffer.addMessage(createMessage(), signature);
				
		// buffer triggered two messages
		assertEquals(2, buffer.collectBuffer().size());
	}
	
	/**
	 * Creates a message with random content
	 */
	private Message createMessage() {
		Random rnd = new Random();

		Message message = new Message();
		message.command(Commands.values()[rnd.nextInt(Commands.values().length)].getNr());
		message.type(Type.values()[rnd.nextInt(Type.values().length)]);
		message.recipient(new PeerAddress(new Number160(rnd)));
		message.sender(new PeerAddress(new Number160(rnd)));
		return message;
	}
	
	private class CountingBufferListener implements MessageBufferListener {

		private int bufferFullTriggerCount;

		public CountingBufferListener() {
			this.bufferFullTriggerCount = 0;
		}

		@Override
		public void bufferFull() {
			// instantly decompose since we don't need to send it here
			bufferFullTriggerCount++;
		}

		public int getTriggerCount() {
			return bufferFullTriggerCount;
		}
	}
}
