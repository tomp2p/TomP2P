package net.tomp2p.relay;

import static org.junit.Assert.assertEquals;
import io.netty.buffer.ByteBuf;

import java.net.InetSocketAddress;
import java.util.ArrayList;
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

public class TestRelayUtils {

	private final SignatureFactory signature = new DSASignatureFactory();

	@Test
	public void composeDecompose() {
		List<Message> messages = new ArrayList<Message>();
		messages.add(createMessage());
		messages.add(createMessage());
		messages.add(createMessage());
		messages.add(createMessage());

		ByteBuf buffer = RelayUtils.composeMessageBuffer(messages, signature);
		List<Message> decomposed = RelayUtils.decomposeCompositeBuffer(buffer, new InetSocketAddress(0), new InetSocketAddress(0), signature);
		
		assertEquals(messages.size(), decomposed.size());
		assertEquals(messages.get(0).messageId(), decomposed.get(0).messageId());
		assertEquals(messages.get(1).messageId(), decomposed.get(1).messageId());
		assertEquals(messages.get(2).messageId(), decomposed.get(2).messageId());
		assertEquals(messages.get(3).messageId(), decomposed.get(3).messageId());
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
}
