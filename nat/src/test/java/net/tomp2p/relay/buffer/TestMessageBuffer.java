package net.tomp2p.relay.buffer;

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
import net.tomp2p.relay.RelayUtils;
import net.tomp2p.relay.UtilsNAT;
import net.tomp2p.relay.buffer.MessageBuffer;
import net.tomp2p.relay.buffer.MessageBufferListener;

import org.junit.Test;
// create three messages
// buffer did not trigger yet
// buffer triggered now
// create one message
// buffer triggered already
// create one message
// buffer did not trigger yet
// wait for the given time and slightly longer
// buffer triggered already
// wait again
// test that buffer did not trigger again
// create five messages
// buffer triggered by now, check the order
// create one message
// garbage collect
// create another message
// buffer triggered two messages
// instantly decompose since we don't need to send it here
public class TestMessageBuffer {

	private final SignatureFactory signature = new DSASignatureFactory();

	@Test
	public void testReachCountLimit() throws InvalidKeyException, SignatureException, IOException {
		CountingBufferListener listener = new CountingBufferListener();
		MessageBuffer<Message> buffer = new MessageBuffer<Message>(3, Long.MAX_VALUE, Long.MAX_VALUE);
		buffer.addListener(listener);

		// create three messages
		Message first = UtilsNAT.createRandomMessage();
		Message second = UtilsNAT.createRandomMessage();
		Message third = UtilsNAT.createRandomMessage();

		
		buffer.addMessage(first, RelayUtils.getMessageSize(first, signature));
		buffer.addMessage(second, RelayUtils.getMessageSize(second, signature));

		// buffer did not trigger yet
		assertEquals(0, listener.getTriggerCount());
		assertEquals(0, listener.getBuffer().size());

		// buffer triggered now
		buffer.addMessage(third, RelayUtils.getMessageSize(third, signature));
		assertEquals(1, listener.getTriggerCount());
		assertEquals(3, listener.getBuffer().size());
	}

	@Test
	public void testReachSizeLimit() throws InvalidKeyException, SignatureException, IOException {
		CountingBufferListener listener = new CountingBufferListener();
		MessageBuffer<Message> buffer = new MessageBuffer<Message>(Integer.MAX_VALUE, 1, Long.MAX_VALUE);
		buffer.addListener(listener);

		// create one message
		buffer.addMessage(UtilsNAT.createRandomMessage(), 2);

		// buffer triggered already
		assertEquals(1, listener.getTriggerCount());
		assertEquals(1, listener.getBuffer().size());
	}

	@Test
	public void testReachAgeLimit() throws InvalidKeyException, SignatureException, IOException, InterruptedException {
		long waitTime = 2000;

		CountingBufferListener listener = new CountingBufferListener();
		MessageBuffer<Message> buffer = new MessageBuffer<Message>(Integer.MAX_VALUE, Long.MAX_VALUE, waitTime);
		buffer.addListener(listener);

		// create one message
		buffer.addMessage(UtilsNAT.createRandomMessage(), 10);

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
	public void testBufferFlush() throws InvalidKeyException, SignatureException, IOException, NoSuchAlgorithmException, InvalidKeySpecException {
		CountingBufferListener listener = new CountingBufferListener();
		MessageBuffer<Message> buffer = new MessageBuffer<Message>(3, Long.MAX_VALUE, Long.MAX_VALUE);
		buffer.addListener(listener);

		// add two messages
		buffer.addMessage(UtilsNAT.createRandomMessage(), 10);
		buffer.addMessage(UtilsNAT.createRandomMessage(), 10);

		buffer.flushNow();
		
		// check whether the buffer has been pre-emptied
		assertEquals(1, listener.getTriggerCount());
		assertEquals(2, listener.getBuffer().size());
	}
	
	@Test
	public void testBufferOrder() throws InvalidKeyException, SignatureException, IOException, NoSuchAlgorithmException, InvalidKeySpecException {
		CountingBufferListener listener = new CountingBufferListener();
		MessageBuffer<Message> buffer = new MessageBuffer<Message>(5, Long.MAX_VALUE, Long.MAX_VALUE);
		buffer.addListener(listener);

		// create five messages
		Message first = UtilsNAT.createRandomMessage();
		Message second = UtilsNAT.createRandomMessage();
		Message third = UtilsNAT.createRandomMessage();
		Message fourth = UtilsNAT.createRandomMessage();
		Message fifth = UtilsNAT.createRandomMessage();

		buffer.addMessage(first, RelayUtils.getMessageSize(first, signature));
		buffer.addMessage(second, RelayUtils.getMessageSize(second, signature));
		buffer.addMessage(third, RelayUtils.getMessageSize(third, signature));
		buffer.addMessage(fourth, RelayUtils.getMessageSize(fourth, signature));
		buffer.addMessage(fifth, RelayUtils.getMessageSize(fifth, signature));
		
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
		MessageBuffer<Message> buffer = new MessageBuffer<Message>(2, Long.MAX_VALUE, Long.MAX_VALUE);
		buffer.addListener(listener);

		// create one message
		buffer.addMessage(UtilsNAT.createRandomMessage(), 10);

		// garbage collect
		System.gc();

		// create another message
		buffer.addMessage(UtilsNAT.createRandomMessage(), 12);
				
		// buffer triggered two messages
		assertEquals(2, listener.getBuffer().size());
	}
	
	private class CountingBufferListener implements MessageBufferListener<Message> {

		private final List<Message> buffer;
		private int bufferFullTriggerCount;

		public CountingBufferListener() {
			this.buffer = new ArrayList<Message>();
			this.bufferFullTriggerCount = 0;
		}

		@Override
		public void bufferFull(List<Message> messages) {
			// instantly decompose since we don't need to send it here
			this.buffer.addAll(messages);
			bufferFullTriggerCount++;
		}

		public int getTriggerCount() {
			return bufferFullTriggerCount;
		}

		public List<Message> getBuffer() {
			return buffer;
		}

		@Override
		public void bufferFlushed(List<Message> messages) {
			bufferFull(messages);
		}
	}
}
