package net.tomp2p.relay.android;

import java.util.List;

import net.tomp2p.connection.Responder;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.relay.RelayUtils;
import net.tomp2p.rpc.DispatchHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferedMessageHandler {

	private static final Logger LOG = LoggerFactory.getLogger(BufferedMessageHandler.class);
	private final Peer peer;

	public BufferedMessageHandler(Peer peer) {
		this.peer = peer;
	}

	/**
	 * Takes the message containing the buffered messages. The buffer is decoded and the requests are executed
	 * 
	 * @param bufferResponse
	 * @param futureDone
	 */
	public void handleBufferResponse(Message bufferResponse, FutureDone<Void> futureDone) {
		Buffer sizeBuffer = bufferResponse.buffer(0);
		Buffer messageBuffer = bufferResponse.buffer(1);
		if (sizeBuffer != null && messageBuffer != null) {
			// decompose the large buffer into a buffer for each message
			List<Buffer> bufferedMessages = MessageBuffer.decomposeCompositeBuffer(sizeBuffer, messageBuffer);
			LOG.debug("Received {} buffered messages", bufferedMessages.size());
			for (Buffer bufferedMessage : bufferedMessages) {
				try {
					Message message = RelayUtils.decodeMessage(bufferedMessage, bufferResponse.recipientSocket(),
							bufferResponse.senderSocket());
					processMessage(message);
				} catch (Exception e) {
					// continue to process the buffers anyway
					LOG.error("Cannot decode the buffer {}", bufferedMessage, e);
				}
			}

			futureDone.done();
		} else {
			LOG.warn("Buffer message does not contain any buffered message");
			futureDone.failed("Cannot find any buffer in the message");
		}
	}

	/**
	 * Execute the message by finding the dispatcher and the responding to the requester
	 * 
	 * @param bufferedMessage the message of the requester to the unreachable peer that was buffered at the
	 *            relay peer.
	 */
	private void processMessage(Message bufferedMessage) {
		DispatchHandler handler = peer.connectionBean().dispatcher().associatedHandler(bufferedMessage);
		if (handler == null) {
			// ignore the message
			LOG.error("Cannot find the associated handler to message {}", bufferedMessage);
			return;
		}

		try {
			handler.handleResponse(bufferedMessage, null, false, new AndroidDirectResponder());
		} catch (Exception e) {
			LOG.error("Cannot handle the buffered message {}", bufferedMessage, e);
		}
	}

	/**
	 * Respond to the original requester (not the relay).
	 * TODO use relay as fallback
	 * 
	 * @author Nico Rutishauser
	 *
	 */
	private class AndroidDirectResponder implements Responder {

		@Override
		public void response(Message responseMessage) {
			// TODO send the response to the requester
			// peer.sendDirect(responseMessage.recipient())
		}

		@Override
		public void failed(Type type, String reason) {
			// TODO Auto-generated method stub

		}

		@Override
		public void responseFireAndForget() {
			// TODO Auto-generated method stub

		}

	}
}
