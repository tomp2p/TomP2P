/*
 * Copyright 2013 Thomas Bocek
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package net.tomp2p.connection;

import java.net.ConnectException;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.concurrent.CancellationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.util.concurrent.GenericFutureListener;
import net.tomp2p.futures.Cancel;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.DataFilter;
import net.tomp2p.message.DataFilterTTL;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.PeerStatusListener;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.ConcurrentCacheMap;
import net.tomp2p.utils.Pair;

/**
 * The class that sends out messages.
 * 
 * @author Thomas Bocek
 * 
 */
public class Sender {

	private static final Logger LOG = LoggerFactory.getLogger(Sender.class);
	private final List<PeerStatusListener> peerStatusListeners;
	private final Dispatcher dispatcher;
	private final DataFilter dataFilterTTL = new DataFilterTTL();

	// this map caches all messages which are meant to be sent by a reverse
	// connection setup
	private final ConcurrentCacheMap<Integer, Pair<FutureResponse, FutureResponse>> cachedRequests = new ConcurrentCacheMap<Integer, Pair<FutureResponse, FutureResponse>>(30, 1024);

	/**
	 * Creates a new sender with the listeners for offline peers.
	 * 
	 * @param peerStatusListeners
	 *            The listener for offline peers
	 * @param channelClientConfiguration
	 *            The configuration used to get the signature factory
	 * @param dispatcher
	 * @param sendBehavior
	 * @param peerBean
	 */
	public Sender(final List<PeerStatusListener> peerStatusListeners, Dispatcher dispatcher) {
		this.peerStatusListeners = peerStatusListeners;
		this.dispatcher = dispatcher;
	}

	
        
        /**
	 * In case a message is sent to the sender itself, this is the cutoff.
	 * 
	 * @param futureResponse
	 *            the future to respond as soon as the proper handler returns it
	 * @param message
	 *            the request
	 */
	public void sendSelf(final FutureResponse futureResponse, final Message message) {
		LOG.debug("Handle message that is intended for the sender itself {}", message);
		message.sendSelf();

		Message copy = message.duplicate(new DataFilter() {
			@Override
			public Data filter(Data data, boolean isConvertMeta, boolean isReply) {
				if (data.isSigned() && data.signature() == null) {
					data.protectEntry(message.privateKey());
				}
				// set new valid from as this data item might have an old one
				data.validFromMillis(System.currentTimeMillis());
				return data.duplicate();
			}
		});

		final DispatchHandler handler = dispatcher.associatedHandler(copy);
		if(handler == null) {
			LOG.error("No handler found for self message {}", message);
			return;
		}
	
		handler.forwardMessage(copy, null, new Responder() {

			@Override
			public FutureDone<Void> response(final Message responseMessage) {
				Message copy = responseMessage.duplicate(dataFilterTTL);
				futureResponse.response(copy);
				return new FutureDone<Void>().done();
			}

			@Override
			public void failed(Type type, String reason) {
				futureResponse.failed("Failed with type " + type.name() + ". Reason: " + reason);
			}

			@Override
			public void responseFireAndForget() {
				futureResponse.emptyResponse();
			}

		});
	}

	/**
	 * After connecting, we check if the connect was successful.
	 * 
	 * @param futureResponse
	 *            The future to set the response
	 * @param message
	 *            The message to send
	 * @param channelFuture
	 *            the future of the connect
	 * @param fireAndForget
	 *            True, if we don't expect a message
	 */
	public FutureResponse sendMessage(final FutureResponse futureResponse, final Message message, final PeerConnection peerConnection,
			final boolean fireAndForget) {
		if (peerConnection.channelFuture() == null) {
			return futureResponse.failed("could not create a " + (message.isUdp() ? "UDP" : "TCP") + " channel");
		}
		LOG.debug("about to connect to {} with channel {}, ff={}, msg={}", message.recipient(), peerConnection.channelFuture().channel(), fireAndForget, message);
		final Cancel connectCancel = createCancel(peerConnection.channelFuture());
		futureResponse.setCancel(connectCancel);
		peerConnection.channelFuture().addListener(new GenericFutureListener<ChannelFuture>() {
			@Override
			public void operationComplete(final ChannelFuture future) throws Exception {
				
				if (future.isSuccess()) {
					final ChannelFuture writeFuture = future.channel().writeAndFlush(message);
					afterSend(writeFuture, futureResponse, fireAndForget, peerConnection);
				} else {
					LOG.warn("Channel creation failed", future.cause());
					LOG.warn("Faild message {}", message);
					futureResponse.failed("Channel creation failed " + future.channel() + "/" + future.cause());
					// may have been closed by the other side,
					// or it may have been canceled from this side
					if (!(future.cause() instanceof CancellationException) && !(future.cause() instanceof ClosedChannelException)
							&& !(future.cause() instanceof ConnectException)) {
						LOG.warn("Channel creation failed to {} for {}", future.channel(), message);
					}
				}
			}
		});
                return futureResponse;
	}
        
        public ChannelFuture sendTCPPeerConnection(PeerConnection peerConnection, ChannelHandler replHandler) {
		// if the channel gets closed, the future should get notified
		ChannelFuture channelFuture = peerConnection.channelFuture();
		// channelCreator can be null if we don't need to create any channels
		ChannelPipeline pipeline = channelFuture.channel().pipeline();
		// we need to replace the handler if this comes from the peer that
		// create a peerConnection, otherwise we
		// need to add a handler
		addOrReplace(pipeline, "dispatcher", "handler", replHandler);
		// uncomment this if the recipient should also heartbeat
		// addIfAbsent(pipeline, "handler", "heartbeat",
		// new HeartBeat(2, pingBuilder).peerConnection(peerConnection));
		return channelFuture;
	}
        
        // private boolean addIfAbsent(ChannelPipeline pipeline, String before,
	// String name,
	// ChannelHandler channelHandler) {
	// List<String> names = pipeline.names();
	// if (names.contains(name)) {
	// return false;
	// } else {
	// if (before == null) {
	// pipeline.addFirst(name, channelHandler);
	// } else {
	// pipeline.addBefore(before, name, channelHandler);
	// }
	// return true;
	// }
	// }
        
        private boolean addOrReplace(ChannelPipeline pipeline, String before, String name, ChannelHandler channelHandler) {
		List<String> names = pipeline.names();
		if (names.contains(name)) {
			pipeline.replace(name, name, channelHandler);
			return false;
		} else {
			if (before == null) {
				pipeline.addFirst(name, channelHandler);
			} else {
				pipeline.addBefore(before, name, channelHandler);
			}
			return true;
		}
	}

	/**
	 * After sending, we check if the write was successful or if it was a fire
	 * and forget.
	 * 
	 * @param writeFuture
	 *            The future of the write operation. Can be UDP or TCP
	 * @param futureResponse
	 *            The future to set the response
	 * @param fireAndForget
	 *            True, if we don't expect a message
	 */
	private void afterSend(final ChannelFuture writeFuture, final FutureResponse futureResponse, 
                final boolean fireAndForget, final PeerConnection peerConnection) {
		final Cancel writeCancel = createCancel(writeFuture);
		futureResponse.setCancel(writeCancel);
		writeFuture.addListener(new GenericFutureListener<ChannelFuture>() {

			@Override
			public void operationComplete(final ChannelFuture future) throws Exception {
				if (!future.isSuccess()) {
                                    LOG.warn("Failed to write to channel - request {} {}.", futureResponse.request(), future.cause());
                                    future.channel().close();
                                    peerConnection.closeListener().failAfterSemaphoreRelease(futureResponse, future.cause());
				}
				if (fireAndForget) {
					LOG.debug("fire and forget, close channel {} now. {}", futureResponse.request(), future.channel());
                                        future.channel().close();
					futureResponse.response(null);
				}
			}
		});

	}

	/**
	 * @param channelFuture
	 *            The channel future that can be canceled
	 * @return Create a cancel class for the channel future
	 */
	private static Cancel createCancel(final ChannelFuture channelFuture) {
		return new Cancel() {
			@Override
			public void cancel() {
				channelFuture.cancel(true);
			}
		};
	}

	
        /**
	 * Get currently cached requests. They are cached because for example the
	 * receiver is behind a NAT. Instead of sending the message directly, a
	 * reverse connection is set up beforehand. After a successful connection
	 * establishment, the cached messages are sent through the direct channel.
	 */
	public ConcurrentCacheMap<Integer, Pair<FutureResponse, FutureResponse>> cachedRequests() {
		return cachedRequests;
	}

	public List<PeerStatusListener> peerStatusListeners() {
		return peerStatusListeners;
	}
}
