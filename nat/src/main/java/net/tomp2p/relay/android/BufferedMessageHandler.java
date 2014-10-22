package net.tomp2p.relay.android;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.List;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC.Commands;
import net.tomp2p.utils.MessageUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferedMessageHandler {

	private static final Logger LOG = LoggerFactory.getLogger(BufferedMessageHandler.class);
	private final Peer peer;
	private final DispatchHandler dispatchHandler;
	private final ConnectionConfiguration connectionConfig;

	public BufferedMessageHandler(Peer peer, DispatchHandler dispatchHandler, ConnectionConfiguration connectionConfig) {
		this.peer = peer;
		this.dispatchHandler = dispatchHandler;
		this.connectionConfig = connectionConfig;
	}

	/**
	 * Takes the message containing the buffered messages. The buffer is decoded and the requests are executed
	 * 
	 * @param bufferResponse
	 * @param futureDone
	 */
	public void handleBufferResponse(Message bufferResponse, FutureDone<Void> futureDone) {
		Buffer buffer = bufferResponse.buffer(0);
		if (buffer != null) {
			// decompose the large buffer into a buffer for each message
			List<Buffer> bufferedMessages = MessageBuffer.decomposeCompositeBuffer(buffer.buffer());
			LOG.debug("Received {} buffered messages", bufferedMessages.size());
			for (Buffer bufferedMessage : bufferedMessages) {
				try {
					Message message = MessageUtils.decodeMessage(bufferedMessage, bufferResponse.recipientSocket(),
							bufferResponse.senderSocket(), peer.connectionBean().channelServer().channelServerConfiguration().signatureFactory());
					boolean isRelay = message.sender().isRelayed();
					if (isRelay && !message.peerSocketAddresses().isEmpty()) {
						PeerAddress tmpSender = message.sender().changePeerSocketAddresses(message.peerSocketAddresses());
						message.sender(tmpSender);
					}
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
			LOG.debug("Handle buffered message {}", bufferedMessage);
			handler.handleResponse(bufferedMessage, null, false, new AndroidDirectResponder());
		} catch (Exception e) {
			LOG.error("Cannot handle the buffered message {}", bufferedMessage, e);
		}
	}

	/**
	 * Respond to the original requester (not the relay).
	 * 
	 * @author Nico Rutishauser
	 *
	 */
	private class AndroidDirectResponder extends SimpleChannelInboundHandler<Message> implements Responder {

		@Override
		public void response(final Message responseMessage) {
			LOG.debug("Send late response {}", responseMessage);
			
			// piggyback the late response. It will be unwrapped by the dispatcher
			Message envelope = dispatchHandler.createMessage(responseMessage.recipient(), Commands.RELAY.getNr(), Type.REQUEST_1);
			envelope.messageId(responseMessage.messageId());
			try {
				envelope.buffer(MessageUtils.encodeMessage(responseMessage, peer.connectionBean().channelServer().channelServerConfiguration().signatureFactory()));
			} catch (Exception e) {
				LOG.error("Cannot wrap the late response into an envelope", e);
			}
			
			FutureResponse futureResponse = MessageUtils.connectAndSend(peer, envelope, connectionConfig);
			futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
				@Override
				public void operationComplete(FutureResponse future) throws Exception {
					if(future.isSuccess()) {
						LOG.debug("Successfully sent late response to requester");
					} else {
						LOG.error("Late response could not be sent to requester. Reason: {}", future.failedReason());
						// TODO use relay as fallback
					}
				}
			});
		}
		
		@Override
		public void failed(Type type, String reason) {
			// TODO send a late message to the requester that the request failed
		}

		@Override
		public void responseFireAndForget() {
			// ignore fire and forget message
		}

		@Override
		protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
			// not used at the moment, we don't send a confirmation to the unreachable peer for the late response
			if(msg.isOk()) {
				LOG.debug("Late message accepted by requester.");
			} else {
				LOG.error("Late requester did not accept the late message. He responded: {}", msg);
			}
		}

	}
}
