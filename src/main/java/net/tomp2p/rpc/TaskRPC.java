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

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Command;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;
import net.tomp2p.task.TaskManager;
import net.tomp2p.task.TaskStatus;
import net.tomp2p.task.Worker;
import net.tomp2p.utils.Utils;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskRPC extends ReplyHandler {
    final private static Logger logger = LoggerFactory.getLogger(TaskRPC.class);

    private final TaskManager taskManager;

    public TaskRPC(PeerBean peerBean, ConnectionBean connectionBean) {
        super(peerBean, connectionBean);
        this.taskManager = peerBean.getTaskManager();
        registerIoHandler(Command.TASK);
    }

    /**
     * Sends a task to a remote peer. This is an RPC.
     * 
     * @param remotePeer
     *            The remote peer to send this request
     * @param channelCreator
     *            The channel creator that creates connections
     * @param forceTCP
     *            Set to true if the communication should be TCP, default is UDP
     * @return The future response to keep track of future events
     */
    public FutureResponse sendTask(final PeerAddress remotePeer, ChannelCreator channelCreator, Number160 taskID,
            Map<Number160, Data> dataMap, Worker mapper, KeyPair keyPair, boolean forceUDP, boolean sign) {
        final Message message = createMessage(remotePeer, Command.TASK, Type.REQUEST_1);
        FutureResponse futureResponse = new FutureResponse(message);
        try {
            byte[] me = Utils.encodeJavaObject(mapper);
            ChannelBuffer payload = ChannelBuffers.wrappedBuffer(me);
            message.setPayload(payload);
            message.setDataMap(dataMap);
            message.setKey(taskID);
            if (sign) {
                message.setPublicKeyAndSign(keyPair);
            }
            if (forceUDP) {
                final RequestHandlerUDP<FutureResponse> requestHandler = new RequestHandlerUDP<FutureResponse>(
                        futureResponse, getPeerBean(), getConnectionBean(), message);
                if (logger.isDebugEnabled()) {
                    logger.debug("send Task " + message);
                }
                return requestHandler.sendUDP(channelCreator);
            } else {
                final RequestHandlerTCP<FutureResponse> requestHandler = new RequestHandlerTCP<FutureResponse>(
                        futureResponse, getPeerBean(), getConnectionBean(), message);
                if (logger.isDebugEnabled()) {
                    logger.debug("send Task " + message);
                }
                return requestHandler.sendTCP(channelCreator);
            }
        } catch (IOException ioe) {
            futureResponse.setFailed(ioe.toString());
            if (logger.isErrorEnabled()) {
                ioe.printStackTrace();
            }
            return futureResponse;
        }
    }

    public FutureResponse sendResult(final PeerAddress remotePeer, ChannelCreator channelCreator, Number160 taskID,
            Map<Number160, Data> dataMap, KeyPair keyPair, boolean forceUDP, boolean sign) {
        final Message message = createMessage(remotePeer, Command.TASK, Type.REQUEST_3);
        FutureResponse futureResponse = new FutureResponse(message);
        if (dataMap != null) {
            message.setDataMap(dataMap);
        }
        message.setKey(taskID);
        if (sign) {
            message.setPublicKeyAndSign(keyPair);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("send Result " + message);
        }
        if (forceUDP) {
            final RequestHandlerUDP<FutureResponse> requestHandler = new RequestHandlerUDP<FutureResponse>(
                    futureResponse, getPeerBean(), getConnectionBean(), message);
            return requestHandler.sendUDP(channelCreator);
        } else {
            final RequestHandlerTCP<FutureResponse> requestHandler = new RequestHandlerTCP<FutureResponse>(
                    futureResponse, getPeerBean(), getConnectionBean(), message);
            return requestHandler.sendTCP(channelCreator);
        }
    }

    public FutureResponse taskStatus(final PeerAddress remotePeer, ChannelCreator channelCreator,
            Collection<Number160> taskIDs, boolean forceTCP) {
        final Message message = createMessage(remotePeer, Command.TASK, Type.REQUEST_2);
        message.setKeys(taskIDs);
        FutureResponse futureResponse = new FutureResponse(message);
        if (!forceTCP) {
            final RequestHandlerUDP<FutureResponse> requestHandler = new RequestHandlerUDP<FutureResponse>(
                    futureResponse, getPeerBean(), getConnectionBean(), message);
            return requestHandler.sendUDP(channelCreator);
        } else {
            final RequestHandlerTCP<FutureResponse> requestHandler = new RequestHandlerTCP<FutureResponse>(
                    futureResponse, getPeerBean(), getConnectionBean(), message);
            return requestHandler.sendTCP(channelCreator);
        }
    }

    @Override
    public Message handleResponse(final Message message, boolean sign) throws Exception {
        if (!((message.getType() == Type.REQUEST_1 || message.getType() == Type.REQUEST_2 || message.getType() == Type.REQUEST_3) && message
                .getCommand() == Command.TASK)) {
            throw new IllegalArgumentException("Message content is wrong");
        }
        if (logger.isDebugEnabled()) {
            logger.debug("send Task - received " + message);
        }
        final Message responseMessage = createResponseMessage(message, Type.OK);
        if (sign) {
            responseMessage.setPublicKeyAndSign(getPeerBean().getKeyPair());
        }
        if (message.getType() == Type.REQUEST_1) {
            Number160 taskId = message.getKey();
            // request 1 is task creation
            Map<Number160, Data> dataMap = message.getDataMap();
            ChannelBuffer channelBuffer = message.getPayload1();
            Object obj = Utils.decodeJavaObject(channelBuffer);
            Worker mapper = (Worker) obj;
            int queuePosition = taskManager.submitTask(getPeerBean().getPeer(), taskId, mapper, dataMap,
                    message.getSender(), sign);
            responseMessage.setInteger(queuePosition);
        } else if (message.getType() == Type.REQUEST_2) {
            Collection<Number160> taskIDs = message.getKeys();
            Map<Number160, Data> dataMap = new HashMap<Number160, Data>();
            for (Number160 taskId : taskIDs) {
                Number320 taskKey = new Number320(taskId, message.getSender().getID());
                TaskStatus taskStatus = taskManager.taskStatus(taskKey);
                Data data = new Data(taskStatus);
                dataMap.put(taskId, data);
            }
            responseMessage.setDataMap(dataMap);
            if (logger.isDebugEnabled()) {
                logger.debug("finished task status for tasks " + taskIDs);
            }
        } else if (message.getType() == Type.REQUEST_3) {
            Number160 taskId = message.getKey();
            Map<Number160, Data> dataMap = message.getDataMap();
            Number320 taskKey = new Number320(taskId, message.getSender().getID());
            taskManager.notifyListeners(taskKey, dataMap);
        } else {
            responseMessage.setType(Type.NOT_FOUND);
        }
        return responseMessage;
    }
}