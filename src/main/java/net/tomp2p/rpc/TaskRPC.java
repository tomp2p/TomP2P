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

import java.io.IOException;
import java.security.KeyPair;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.mapreduce.Mapper;
import net.tomp2p.mapreduce.TaskManager;
import net.tomp2p.mapreduce.TaskStatus;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Command;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Utils;

public class TaskRPC extends ReplyHandler
{
	private final TaskManager taskManager;
	
	public TaskRPC(PeerBean peerBean, ConnectionBean connectionBean, TaskManager taskManager)
	{
		super(peerBean, connectionBean);
		this.taskManager = taskManager;
		registerIoHandler(Command.TASK);
	}

	/**
	 * Sends a task to a remote peer. This is an RPC.
	 * 
	 * @param remotePeer The remote peer to send this request
	 * @param channelCreator The channel creator that creates connections
	 * @param forceTCP Set to true if the communication should be TCP, default
	 *        is UDP
	 * @return The future response to keep track of future events
	 */
	public FutureResponse task(final PeerAddress remotePeer, ChannelCreator channelCreator, Number160 taskID, 
			Map<Number160, Data> dataMap, Mapper mapper, KeyPair keyPair, boolean forceUDP)
	{
		final Message message = createMessage(remotePeer, Command.TASK, Type.REQUEST_1);
		FutureResponse futureResponse = new FutureResponse(message);
		try
		{
			byte[] me = Utils.encodeJavaObject(mapper);
			ChannelBuffer payload= ChannelBuffers.wrappedBuffer(me);
			message.setPayload(payload);
			message.setDataMap(dataMap);
			message.setKey(taskID);
			if(keyPair != null)
			{
				message.setPublicKeyAndSign(keyPair);
			}
			if(forceUDP)
			{
				final RequestHandlerUDP<FutureResponse> requestHandler = new RequestHandlerUDP<FutureResponse>(futureResponse, peerBean, connectionBean, message);
				return requestHandler.sendUDP(channelCreator);
			}
			else
			{
				final RequestHandlerTCP<FutureResponse> requestHandler = new RequestHandlerTCP<FutureResponse>(futureResponse, peerBean, connectionBean, message);
				return requestHandler.sendTCP(channelCreator);
			}
		}
		catch(IOException ioe)
		{
			futureResponse.setFailed(ioe.toString());
			return futureResponse;
		}
	}
	
	public FutureResponse taskStatus(final PeerAddress remotePeer, ChannelCreator channelCreator, Collection<Number160> taskIDs, boolean forceTCP)
	{
		final Message message = createMessage(remotePeer, Command.TASK, Type.REQUEST_2);
		message.setKeys(taskIDs);
		FutureResponse futureResponse = new FutureResponse(message);
		if(!forceTCP)
		{
			final RequestHandlerUDP<FutureResponse> requestHandler = new RequestHandlerUDP<FutureResponse>(futureResponse, peerBean, connectionBean, message);
			return requestHandler.sendUDP(channelCreator);
		}
		else
		{
			final RequestHandlerTCP<FutureResponse> requestHandler = new RequestHandlerTCP<FutureResponse>(futureResponse, peerBean, connectionBean, message);
			return requestHandler.sendTCP(channelCreator);
		}
	}

	@Override
	public boolean checkMessage(final Message message)
	{
		return (message.getType() == Type.REQUEST_1 || message.getType() == Type.REQUEST_2) 
				&& message.getCommand() == Command.TASK;
	}

	@Override
	public Message handleResponse(final Message message, boolean sign) throws Exception
	{
		final Message responseMessage = createMessage(message.getSender(), message.getCommand(),
				Type.OK);
		
		if(message.getType() == Type.REQUEST_1)
		{
			Number160 taskId = message.getKey();
			// request 1 is task creation
			Map<Number160, Data> dataMap = message.getDataMap();
			ChannelBuffer channelBuffer = message.getPayload1();
			Object obj = Utils.decodeJavaObject(channelBuffer.array(), channelBuffer.arrayOffset(),
					channelBuffer.capacity());
			Mapper mapper = (Mapper) obj;
			int queuePosition = taskManager.submitTask(taskId, mapper, dataMap);
			responseMessage.setInteger(queuePosition);
		}
		else if(message.getType() == Type.REQUEST_2)
		{
			Collection<Number160> taskIDs = message.getKeys();
			for(Number160 taskId: taskIDs)
			{
				TaskStatus taskStatus = taskManager.taskStatus(taskId);
				Data data = new Data(taskStatus);
				Map<Number160, Data> resultMap = new HashMap<Number160, Data>();
				resultMap.put(taskId, data);
				message.setDataMap(resultMap);
			}
		}
		else
		{
			responseMessage.setType(Type.NOT_FOUND);
		}
		return responseMessage;
	}
}
