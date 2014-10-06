package net.tomp2p.relay.android;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.List;

import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.futures.FutureResponse;
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
	private class AndroidDirectResponder extends SimpleChannelInboundHandler<Message> implements Responder {

		@Override
		public void response(final Message responseMessage) {
			// send the response to the requester
			LOG.debug("Send late response {}", responseMessage);
			FuturePeerConnection futurePeerConnection = peer.createPeerConnection(responseMessage.recipient());
			futurePeerConnection.addListener(new BaseFutureAdapter<FutureDone<PeerConnection>>() {
				@Override
				public void operationComplete(FutureDone<PeerConnection> futureDone) throws Exception {
					if(futureDone.isSuccess()) {
						sendThroughOpenConnection(responseMessage, futureDone.object());
					} else {
						LOG.error("Could not send late respose because could not open a peer connection");
					}
				}
			});
		}
		
		private void sendThroughOpenConnection(final Message responseMessage, final PeerConnection peerConnection) {
			final FutureResponse futureResponse = new FutureResponse(responseMessage);
			FutureChannelCreator futureChannelCreator2 = peerConnection.acquire(futureResponse);
			futureChannelCreator2.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
				@Override
				public void operationComplete(FutureChannelCreator future) throws Exception {
					if (future.isSuccess()) {
						peer.connectionBean().sender().sendTCP(AndroidDirectResponder.this, futureResponse, responseMessage, future.channelCreator(), ConnectionBean.DEFAULT_TCP_IDLE_SECONDS, ConnectionBean.DEFAULT_CONNECTION_TIMEOUT_TCP, peerConnection);
					} else {
						LOG.error("Could not acquire channel to send late response. Reason: {}", future.failedReason());
						futureResponse.failed("Could not acquire channel to send late response");
					}
				}
			});
			
			futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
				@Override
				public void operationComplete(FutureResponse future) throws Exception {
					if(future.isSuccess()) {
						LOG.debug("Successfully sent late response to requester");
					} else {
						LOG.error("Late response could not be sent to requester. Reason: {}", future.failedReason());
					}
					// close the peer connection as soon as the message is sent (or failed to send)
					peerConnection.close();
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
			if(msg.isOk()) {
				LOG.debug("Late message accepted by requester.");
			} else {
				LOG.error("Late requester did not accept the late message. He responded: {}", msg);
			}
		}

	}
}
