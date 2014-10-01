package net.tomp2p.relay.android;

import static org.junit.Assert.assertEquals;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;

import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.relay.RelayUtils;
import net.tomp2p.relay.UtilsNAT;

import org.junit.Test;

/**
 * Tests the composite buffer behavior. When relaying Android devices, messages need to be buffered. These
 * messages are consolidated to a single (large) buffer object at the sender side and sliced at the receiver
 * side. This class tests the correct segmentation and reassembly behavior.
 * 
 * @author Nico Rutishauser
 *
 */
public class TestBufferComposing {

	@Test
	public void testBufferComposing() throws InvalidKeyException, SignatureException, IOException, NoSuchAlgorithmException,
			InvalidKeySpecException {
		// create three messages
		Message first = UtilsNAT.createRandomMessage();
		Message second = UtilsNAT.createRandomMessage();
		Message third = UtilsNAT.createRandomMessage();

		Buffer firstBuffer = RelayUtils.encodeMessage(first);
		Buffer secondBuffer = RelayUtils.encodeMessage(second);
		Buffer thirdBuffer = RelayUtils.encodeMessage(third);

		// buffer the buffers
		CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
		compositeByteBuf.addComponent(firstBuffer.buffer());
		compositeByteBuf.addComponent(secondBuffer.buffer());
		compositeByteBuf.addComponent(thirdBuffer.buffer());

		// consolidate all components into one
		compositeByteBuf.consolidate();
		byte[] rawComposedData = compositeByteBuf.array();
		assertEquals(firstBuffer.length() + secondBuffer.length() + thirdBuffer.length(), rawComposedData.length);

		/** now imagine that the array is sent to another peer... */

		// wrap the 'received' bytes and try to
		ByteBuf receivedBuffer = Unpooled.wrappedBuffer(rawComposedData);
		ByteBuf firstReceivedBuffer = receivedBuffer.slice(0, firstBuffer.length());
		ByteBuf secondReceivedBuffer = receivedBuffer.slice(firstBuffer.length(), secondBuffer.length());
		ByteBuf thirdReceivedBuffer = receivedBuffer.slice(firstBuffer.length() + secondBuffer.length(),
				thirdBuffer.length());

		assertEquals(firstReceivedBuffer.readableBytes(), firstBuffer.length());
		assertEquals(secondReceivedBuffer.readableBytes(), secondBuffer.length());
		assertEquals(thirdReceivedBuffer.readableBytes(), thirdBuffer.length());

		// compare the buffers
		Message firstReceived = RelayUtils.decodeMessage(new Buffer(firstReceivedBuffer), first.recipientSocket(),
				first.senderSocket());
		assertEquals(first.messageId(), firstReceived.messageId());
		assertEquals(first.sender(), firstReceived.sender());

		Message secondReceived = RelayUtils.decodeMessage(new Buffer(secondReceivedBuffer), second.recipientSocket(),
				second.senderSocket());
		assertEquals(second.messageId(), secondReceived.messageId());
		assertEquals(second.sender(), secondReceived.sender());

		Message thirdReceived = RelayUtils.decodeMessage(new Buffer(thirdReceivedBuffer), third.recipientSocket(),
				third.senderSocket());
		assertEquals(third.messageId(), thirdReceived.messageId());
		assertEquals(third.sender(), thirdReceived.sender());
	}
}
