package net.tomp2p.relay.android;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.RelayUtils;
import net.tomp2p.rpc.RPC.Commands;

import org.junit.Test;

public class TestMessageBuffer {

	@Test
	public void testReachCountLimit() throws InvalidKeyException, SignatureException, IOException {
		CountingBufferListener listener = new CountingBufferListener();
		MessageBuffer buffer = new MessageBuffer(3, Long.MAX_VALUE, Long.MAX_VALUE, listener);

		// create three messages
		Message first = createMessage();
		Message second = createMessage();
		Message third = createMessage();

		buffer.addMessage(first);
		buffer.addMessage(second);

		// buffer did not trigger yet
		assertEquals(0, listener.getTriggerCount());
		assertEquals(0, listener.getBuffer().size());

		// buffer triggered now
		buffer.addMessage(third);
		assertEquals(1, listener.getTriggerCount());
		assertEquals(3, listener.getBuffer().size());
	}

	@Test
	public void testReachSizeLimit() throws InvalidKeyException, SignatureException, IOException {
		CountingBufferListener listener = new CountingBufferListener();
		MessageBuffer buffer = new MessageBuffer(Integer.MAX_VALUE, 1, Long.MAX_VALUE, listener);

		// create one message
		buffer.addMessage(createMessage());

		// buffer triggered already
		assertEquals(1, listener.getTriggerCount());
		assertEquals(1, listener.getBuffer().size());
	}

	@Test
	public void testReachAgeLimit() throws InvalidKeyException, SignatureException, IOException, InterruptedException {
		long waitTime = 2000;

		CountingBufferListener listener = new CountingBufferListener();
		MessageBuffer buffer = new MessageBuffer(Integer.MAX_VALUE, Long.MAX_VALUE, waitTime, listener);

		// create one message
		buffer.addMessage(createMessage());

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
		MessageBuffer buffer = new MessageBuffer(5, Long.MAX_VALUE, Long.MAX_VALUE, listener);

		// create five messages
		Message first = createMessage();
		Message second = createMessage();
		Message third = createMessage();
		Message fourth = createMessage();
		Message fifth = createMessage();

		buffer.addMessage(first);
		buffer.addMessage(second);
		buffer.addMessage(third);
		buffer.addMessage(fourth);
		buffer.addMessage(fifth);
		
		// buffer triggered by now, check the order
		List<Buffer> content = listener.getBuffer();
		assertEquals(first.messageId(), RelayUtils.decodeMessage(content.get(0), new InetSocketAddress(0), new InetSocketAddress(0)).messageId());
		assertEquals(second.messageId(), RelayUtils.decodeMessage(content.get(1), new InetSocketAddress(0), new InetSocketAddress(0)).messageId());
		assertEquals(third.messageId(), RelayUtils.decodeMessage(content.get(2), new InetSocketAddress(0), new InetSocketAddress(0)).messageId());
		assertEquals(fourth.messageId(), RelayUtils.decodeMessage(content.get(3), new InetSocketAddress(0), new InetSocketAddress(0)).messageId());
		assertEquals(fifth.messageId(), RelayUtils.decodeMessage(content.get(4), new InetSocketAddress(0), new InetSocketAddress(0)).messageId());
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

		private final List<Buffer> buffer;
		private int bufferFullTriggerCount;

		public CountingBufferListener() {
			this.buffer = new ArrayList<Buffer>();
			this.bufferFullTriggerCount = 0;
		}

		@Override
		public void bufferFull(Buffer sizeBuffer, Buffer messageBuffer) {
			// instantly decompose since we don't need to send it here
			this.buffer.addAll(MessageBuffer.decomposeCompositeBuffer(sizeBuffer, messageBuffer));
			bufferFullTriggerCount++;
		}

		public int getTriggerCount() {
			return bufferFullTriggerCount;
		}

		public List<Buffer> getBuffer() {
			return buffer;
		}
	}
}
