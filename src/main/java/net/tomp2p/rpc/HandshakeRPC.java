/*
 * Copyright 2009 Thomas Bocek
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
package net.tomp2p.rpc;

import java.util.ArrayList;
import java.util.Collection;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Command;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Timings;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HandshakeRPC extends ReplyHandler
{
	final private static Logger logger = LoggerFactory.getLogger(HandshakeRPC.class);
	final private boolean enable;
	final private boolean wait;

	public HandshakeRPC(PeerBean peerBean, ConnectionBean connectionBean)
	{
		this(peerBean, connectionBean, true, true, false);
	}

	HandshakeRPC(PeerBean peerBean, ConnectionBean connectionBean, final boolean enable, final boolean register,
			final boolean wait)
	{
		super(peerBean, connectionBean);
		this.enable = enable;
		this.wait = wait;
		if (register)
			registerIoHandler(Command.PING);
	}

	public FutureResponse pingBroadcastUDP(final PeerAddress remotePeer, ChannelCreator channelCreator)
	{
		return createHandlerUDP(remotePeer, Type.REQUEST_1).sendBroadcastUDP(channelCreator);
	}
	
	public RequestHandlerUDP<FutureResponse> pingUDP(final PeerAddress remotePeer)
	{
		return createHandlerUDP(remotePeer, Type.REQUEST_1);
	}

	public FutureResponse pingUDP(final PeerAddress remotePeer, ChannelCreator channelCreator)
	{
		return pingUDP(remotePeer).sendUDP(channelCreator);
	}

	public FutureResponse pingTCP(final PeerAddress remotePeer, ChannelCreator channelCreator)
	{
		return createHandlerTCP(remotePeer, Type.REQUEST_1).sendTCP(channelCreator);
	}

	public FutureResponse fireUDP(final PeerAddress remotePeer, ChannelCreator channelCreator)
	{
		return createHandlerUDP(remotePeer, Type.REQUEST_FF_1).fireAndForgetUDP(channelCreator);
	}

	public FutureResponse fireTCP(final PeerAddress remotePeer, ChannelCreator channelCreator)
	{
		return createHandlerTCP(remotePeer, Type.REQUEST_FF_1).fireAndForgetTCP(channelCreator);
	}

	private RequestHandlerUDP<FutureResponse> createHandlerUDP(final PeerAddress remotePeer, Type type)
	{
		final Message message = createMessage(remotePeer, Command.PING, type);
		FutureResponse futureResponse = new FutureResponse(message);
		return new RequestHandlerUDP<FutureResponse>(futureResponse, peerBean, connectionBean, message);
	}

	private RequestHandlerTCP<FutureResponse> createHandlerTCP(final PeerAddress remotePeer, Type type)
	{
		final Message message = createMessage(remotePeer, Command.PING, type);
		FutureResponse futureResponse = new FutureResponse(message);
		return new RequestHandlerTCP<FutureResponse>(futureResponse, peerBean, connectionBean, message);
	}

	public FutureResponse pingUDPDiscover(final PeerAddress remotePeer, ChannelCreator channelCreator)
	{
		final Message message = createMessage(remotePeer, Command.PING, Type.REQUEST_2);
		Collection<PeerAddress> self = new ArrayList<PeerAddress>();
		self.add(peerBean.getServerPeerAddress());
		message.setNeighbors(self);
		FutureResponse futureResponse = new FutureResponse(message);
		return new RequestHandlerUDP<FutureResponse>(futureResponse, peerBean, connectionBean, message).sendUDP(channelCreator);
	}

	public FutureResponse pingTCPDiscover(final PeerAddress remotePeer, ChannelCreator channelCreator)
	{
		final Message message = createMessage(remotePeer, Command.PING, Type.REQUEST_2);
		Collection<PeerAddress> self = new ArrayList<PeerAddress>();
		self.add(peerBean.getServerPeerAddress());
		message.setNeighbors(self);
		FutureResponse futureResponse = new FutureResponse(message);
		return new RequestHandlerTCP<FutureResponse>(futureResponse, peerBean, connectionBean, message).sendTCP(channelCreator);
	}

	public FutureResponse pingUDPProbe(final PeerAddress remotePeer, ChannelCreator channelCreator)
	{
		final Message message = createMessage(remotePeer, Command.PING, Type.REQUEST_3);
		FutureResponse futureResponse = new FutureResponse(message);
		return new RequestHandlerUDP<FutureResponse>(futureResponse, peerBean, connectionBean, message).sendUDP(channelCreator);
	}

	public FutureResponse pingTCPProbe(final PeerAddress remotePeer, ChannelCreator channelCreator)
	{
		final Message message = createMessage(remotePeer, Command.PING, Type.REQUEST_3);
		FutureResponse futureResponse = new FutureResponse(message);
		return new RequestHandlerTCP<FutureResponse>(futureResponse, peerBean, connectionBean, message).sendTCP(channelCreator);
	}

	@Override
	public boolean checkMessage(final Message message)
	{
		return (message.getType() == Type.REQUEST_FF_1 || message.getType() == Type.REQUEST_1 || message.getType() == Type.REQUEST_2 || message.getType() == Type.REQUEST_3)
				&& message.getCommand() == Command.PING;
	}

	@Override
	public Message handleResponse(final Message message, boolean sign) throws Exception
	{
		// probe
		if (message.getType() == Type.REQUEST_3)
		{
			logger.debug("reply to probing, fire message to " + message.getSender());
			final Message responseMessage = createMessage(message.getSender(), Command.PING, Type.OK);
			if (sign)
			{
				responseMessage.setPublicKeyAndSign(peerBean.getKeyPair());
			}
			responseMessage.setMessageId(message.getMessageId());
			if (message.isUDP()) 
			{
				connectionBean.getConnectionReservation().reserve(1).addListener(new BaseFutureAdapter<FutureChannelCreator>()
				{
					@Override
					public void operationComplete(FutureChannelCreator future) throws Exception
					{
						if(future.isSuccess())
						{
							FutureResponse futureResponse=fireUDP(message.getSender(), future.getChannelCreator());
							Utils.addReleaseListenerAll(futureResponse, connectionBean.getConnectionReservation(), future.getChannelCreator());
						}
						else
						{
							if(logger.isWarnEnabled())
							{
								logger.warn("handleResponse for REQUEST_3 failed (UDP) "+future.getFailedReason());
							}
						}		
					}
				});
			}
			else 
			{
				connectionBean.getConnectionReservation().reserve(1).addListener(new BaseFutureAdapter<FutureChannelCreator>()
				{
					@Override
					public void operationComplete(FutureChannelCreator future) throws Exception
					{
						if(future.isSuccess())
						{
							FutureResponse futureResponse=fireTCP(message.getSender(), future.getChannelCreator());
							Utils.addReleaseListenerAll(futureResponse, connectionBean.getConnectionReservation(), future.getChannelCreator());
						}
						else
						{
							if(logger.isWarnEnabled())
							{
								logger.warn("handleResponse for REQUEST_3 failed (TCP) "+future.getFailedReason());
							}
						}
					}
				});
			}
			//we are here optimistic
			return responseMessage;
		}
		// discover
		else if (message.getType() == Type.REQUEST_2)
		{
			if(logger.isDebugEnabled()) {
				logger.debug("reply to discover, found " + message.getSender());
			}
			final Message responseMessage = createMessage(message.getSender(), Command.PING, Type.OK);
			if (sign)
			{
				responseMessage.setPublicKeyAndSign(peerBean.getKeyPair());
			}
			responseMessage.setMessageId(message.getMessageId());
			Collection<PeerAddress> self = new ArrayList<PeerAddress>();
			self.add(message.getSender());
			responseMessage.setNeighbors(self);
			return responseMessage;
		}
		// regular ping
		else  if (message.getType() == Type.REQUEST_1)
		{
			//test if this is a broadcast message to ourselfs. If it is, do not reply.
			if (message.getSender().getID().equals(peerBean.getServerPeerAddress().getID())
					&& message.getRecipient().getID().equals(Number160.ZERO))
			{
				return message;
			}
			if (enable)
			{
				final Message responseMessage = createMessage(message.getSender(), Command.PING, Type.OK);
				if (sign)
				{
					responseMessage.setPublicKeyAndSign(peerBean.getKeyPair());
				}
				responseMessage.setMessageId(message.getMessageId());
				if (wait)
				{
					Timings.sleepUninterruptibly(10 * 1000);
				}
				return responseMessage;
			}
			else
			{
				logger.debug("do not reply");
				if (wait)
				{
					Timings.sleepUninterruptibly(10 * 1000);
				}
				return null;
			}
		}
		// fire and forget
		else 
		{
			return message;
		}
	}
}