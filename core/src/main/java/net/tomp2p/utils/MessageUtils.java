package net.tomp2p.utils;

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

import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.RequestHandler;
import net.tomp2p.connection.SignatureFactory;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Decoder;
import net.tomp2p.message.Encoder;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.storage.AlternativeCompositeByteBuf;

public class MessageUtils {

	private static Charset charset = Charset.forName("UTF-8");
	private static CharsetEncoder encoder = charset.newEncoder();
	private static CharsetDecoder decoder = charset.newDecoder();

	private MessageUtils() {
		// only static methods
	}
	
	/**
	 * Encodes a message into a buffer, such that it can be used as a message payload (piggybacked), stored, etc.
	 */
	public static Buffer encodeMessage(Message message, SignatureFactory signatureFactory) throws InvalidKeyException, SignatureException, IOException {
		Encoder e = new Encoder(signatureFactory);
		AlternativeCompositeByteBuf buf = AlternativeCompositeByteBuf.compBuffer();
		e.write(buf, message, message.receivedSignature());
		return new Buffer(buf);
	}

	/**
	 * Decodes a message which was encoded using {{@link #encodeMessage(Message, SignatureFactory)}}.
	 */
	public static Message decodeMessage(Buffer buf, InetSocketAddress recipient, InetSocketAddress sender, SignatureFactory signatureFactory)
	        throws InvalidKeyException, NoSuchAlgorithmException, InvalidKeySpecException, SignatureException, IOException {
		Decoder d = new Decoder(signatureFactory);
		final int readerBefore = buf.buffer().readerIndex();
		d.decodeHeader(buf.buffer(), recipient, sender);
		final boolean donePayload = d.decodePayload(buf.buffer());
		d.decodeSignature(buf.buffer(), readerBefore, donePayload);
		return d.message();
	}
	
	/**
	 * Calculates the size of the message
	 */
	public static int getMessageSize(Message message, SignatureFactory signatureFactory) throws InvalidKeyException, SignatureException, IOException {
		// TODO instead of real encoding, calculate it using the content references
		int size = encodeMessage(message, signatureFactory).length();
		message.restoreContentReferences();
		message.restoreBuffers();
		return size;
	}

	/**
	 * Encodes any String into a buffer to send it with a message
	 * 
	 * @param content the String to encode into a buffer
	 * @return a buffer containing the (encoded) String.
	 */
	public static Buffer encodeString(String content) {
		if (content == null) {
			return null;
		}

		ByteBuffer byteBuffer;
		synchronized (encoder) {
			encoder.reset();
			try {
				byteBuffer = encoder.encode(CharBuffer.wrap(content));
			} catch (CharacterCodingException e) {
				return null;
			}
			encoder.flush(byteBuffer);
		}
		ByteBuf wrappedBuffer = Unpooled.wrappedBuffer(byteBuffer);
		return new Buffer(wrappedBuffer);
	}

	/**
	 * Decodes buffer containing a String
	 * 
	 * @param buffer the buffer received in a message
	 * @return the encodeed String
	 */
	public static String decodeString(Buffer buffer) {
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
	 * Send a Message from one Peer to another Peer internally. This avoids the
	 * overhead of sendDirect.
	 */
	private static void send(final PeerConnection peerConnection, PeerBean peerBean, ConnectionBean connectionBean, ConnectionConfiguration config, final FutureResponse futureResponse) {
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
	}

	/**
	 * Send a Message from one Peer to another Peer internally. This avoids the
	 * overhead of sendDirect. This Method is used for relaying and reverse
	 * Connection setup.
	 * @return the response
	 */
	public static FutureResponse send(final PeerConnection peerConnection, PeerBean peerBean, ConnectionBean connectionBean, ConnectionConfiguration config, Message message) {
		final FutureResponse futureResponse = new FutureResponse(message);
		send(peerConnection, peerBean, connectionBean, config, futureResponse);
		return futureResponse;
	}
	
	/**
	 * Opens a new peer connection to the receiver and sends the message through it.
	 * @param peer
	 * @param message
	 * @param config
	 * @return
	 */
	public static FutureResponse connectAndSend(final Peer peer, final Message message, final ConnectionConfiguration config) {
		final FutureResponse futureResponse = new FutureResponse(message);
		final FuturePeerConnection fpc = peer.createPeerConnection(message.recipient());
		fpc.addListener(new BaseFutureAdapter<FuturePeerConnection>() {
            public void operationComplete(final FuturePeerConnection futurePeerConnection) throws Exception {
                if (futurePeerConnection.isSuccess()) {
                	// successfully created a connection to the other peer
                	final PeerConnection peerConnection = futurePeerConnection.object();
                	
                	// send the message
                	send(peerConnection, peer.peerBean(), peer.connectionBean(), config, futureResponse);
                } else {
                    futureResponse.failed(fpc);
                }
            }
        });
		
		return futureResponse;
	}
}
