package net.tomp2p.relay;

import static org.junit.Assert.assertEquals;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.List;

import net.tomp2p.connection.DSASignatureFactory;
import net.tomp2p.connection.SignatureFactory;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;

import org.junit.Assert;
import org.junit.Test;

public class TestRelayUtils {

	private final SignatureFactory signature = new DSASignatureFactory();

	@Test
	public void composeDecompose() {
		List<Message> messages = new ArrayList<Message>();
		messages.add(UtilsNAT.createRandomMessage());
		messages.add(UtilsNAT.createRandomMessage());
		messages.add(UtilsNAT.createRandomMessage());
		messages.add(UtilsNAT.createRandomMessage());

		ByteBuf buffer = RelayUtils.composeMessageBuffer(messages, signature);
		List<Message> decomposed = RelayUtils.decomposeCompositeBuffer(buffer, new InetSocketAddress(0), new InetSocketAddress(0), signature);
		
		assertEquals(messages.size(), decomposed.size());
		assertEquals(messages.get(0).messageId(), decomposed.get(0).messageId());
		assertEquals(messages.get(1).messageId(), decomposed.get(1).messageId());
		assertEquals(messages.get(2).messageId(), decomposed.get(2).messageId());
		assertEquals(messages.get(3).messageId(), decomposed.get(3).messageId());
	}
	

	@Test
	public void testEncodeDecodeRelayedMessage() throws InvalidKeyException, SignatureException, IOException, NoSuchAlgorithmException,
			InvalidKeySpecException {
		Message message = UtilsNAT.createRandomMessage();

		List<PeerSocketAddress> relays = new ArrayList<PeerSocketAddress>();
		relays.add(new PeerSocketAddress(InetAddress.getLocalHost(), 8000, 9000));
		relays.add(new PeerSocketAddress(InetAddress.getLocalHost(), 8001, 9001));
		relays.add(new PeerSocketAddress(InetAddress.getLocalHost(), 8002, 9002));

		PeerAddress sender = UtilsNAT.createAddress().changeRelayed(true).changePeerSocketAddresses(relays)
				.changeFirewalledTCP(true).changeFirewalledUDP(true);
		message.sender(sender);
		message.senderSocket(sender.createSocketTCP());
		
		PeerAddress receiver = UtilsNAT.createAddress();
		message.recipient(receiver);
		message.recipientSocket(receiver.createSocketTCP());
		
		Buffer encoded = RelayUtils.encodeMessage(message, signature);
		Message decoded = RelayUtils.decodeMessage(encoded, message.recipientSocket(), message.senderSocket(), signature);
		Assert.assertEquals(message.peerSocketAddresses().size(), decoded.peerSocketAddresses().size());
	}
	
	@Test
	public void testEncodeDecodeString() {
		String test = "dummy";
		Buffer encoded = RelayUtils.encodeString(test);
		String decoded = RelayUtils.decodeString(encoded);
		Assert.assertEquals(test, decoded);
	}
}
