package net.tomp2p.relay;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.RequestHandler;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Decoder;
import net.tomp2p.message.Encoder;
import net.tomp2p.message.Message;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
import net.tomp2p.peers.PeerStatatistic;
import net.tomp2p.storage.AlternativeCompositeByteBuf;

public class RelayUtils {

	private static Charset charset = Charset.forName("UTF-8");
	private static CharsetEncoder encoder = charset.newEncoder();
	private static CharsetDecoder decoder = charset.newDecoder();

	private RelayUtils() {
		// only static methods
	}
	
	public static Buffer encodeMessage(Message message) throws InvalidKeyException, SignatureException, IOException {
		Encoder e = new Encoder(null);
		AlternativeCompositeByteBuf buf = AlternativeCompositeByteBuf.compBuffer();
		e.write(buf, message);
		return new Buffer(buf);
	}

	public static Message decodeMessage(Buffer buf, InetSocketAddress recipient, InetSocketAddress sender)
			throws InvalidKeyException, NoSuchAlgorithmException, InvalidKeySpecException {
		Decoder d = new Decoder(null);
		d.decodeHeader(buf.buffer(), recipient, sender);
		d.decodePayload(buf.buffer());
		return d.message();
	}

	public static List<Map<Number160, PeerStatatistic>> unflatten(Collection<PeerAddress> map, PeerAddress sender) {
		PeerMapConfiguration peerMapConfiguration = new PeerMapConfiguration(sender.peerId());
		PeerMap peerMap = new PeerMap(peerMapConfiguration);
		for (PeerAddress peerAddress : map) {
			peerMap.peerFound(peerAddress, null, null);
		}
		return peerMap.peerMapVerified();
	}

	public static Collection<PeerAddress> flatten(List<Map<Number160, PeerStatatistic>> maps) {
		Collection<PeerAddress> result = new ArrayList<PeerAddress>();
		for (Map<Number160, PeerStatatistic> map : maps) {
			for (PeerStatatistic peerStatatistic : map.values()) {
				result.add(peerStatatistic.peerAddress());
			}
		}
		return result;
	}

	/**
	 * Encodes the Google Cloud messaging registration ID into a buffer to send it to the relay
	 * 
	 * @param registrationId the registration ID.
	 * @return a buffer containing the (encoded) registration ID.
	 */
	public static Buffer encodeRegistrationId(String registrationId) {
		if (registrationId == null) {
			return null;
		}

		ByteBuffer byteBuffer;
		synchronized (encoder) {
			encoder.reset();
			try {
				byteBuffer = encoder.encode(CharBuffer.wrap(registrationId));
			} catch (CharacterCodingException e) {
				return null;
			}
			encoder.flush(byteBuffer);
		}
		ByteBuf wrappedBuffer = Unpooled.wrappedBuffer(byteBuffer);
		return new Buffer(wrappedBuffer);
	}

	/**
	 * Decodes the Google Cloud messaging registration ID fro a buffer
	 * 
	 * @param buffer the buffer received at the relay peer
	 * @return the registration ID of the device
	 */
	public static String decodeRegistrationId(Buffer buffer) {
		if (buffer == null || buffer.buffer() == null) {
			return null;
		}

		ByteBuffer nioBuffer = buffer.buffer().nioBuffer();
		synchronized (decoder) {
			decoder.reset();
			CharBuffer decoded;
			try {
				decoded = decoder.decode(nioBuffer);
			} catch (CharacterCodingException e) {
				return null;
			}
			decoder.flush(decoded);
			return decoded.toString();
		}
	}

	/**
	 * Send a message through an open peer connection (non-blocking)
	 * @return the response
	 */
	public static FutureResponse send(final PeerConnection peerConnection, PeerBean peerBean, ConnectionBean connectionBean, ConnectionConfiguration config, Message message) {
		final FutureResponse futureResponse = new FutureResponse(message);
		final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(futureResponse, peerBean, connectionBean, config);
		final FutureChannelCreator fcc = peerConnection.acquire(futureResponse);
		fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
			@Override
			public void operationComplete(FutureChannelCreator future) throws Exception {
				if (future.isSuccess()) {
					requestHandler.sendTCP(peerConnection.channelCreator(), peerConnection);
				} else {
					futureResponse.failed(future);
				}
			}
		});

		return futureResponse;
	}
}
