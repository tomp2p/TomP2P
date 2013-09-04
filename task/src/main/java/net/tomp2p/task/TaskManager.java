/*
 * Copyright 2012 Thomas Bocek
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

package net.tomp2p.task;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.tomp2p.connection2.ConnectionBean;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DigestInfo;
import net.tomp2p.rpc.TaskRPC;
import net.tomp2p.storage.Data;
import net.tomp2p.task.TaskStatus.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskManager {
    final private static Logger logger = LoggerFactory.getLogger(TaskManager.class);

    final private ConnectionBean connectionBean;

    final private Object lock = new Object();

    final private ThreadPoolExecutor executor;

    // may grow
    final private Map<Number320, Status> status = new HashMap<Number320, Status>();

    final private Map<Number320, String> exceptions = new HashMap<Number320, String>();

    final private Collection<TaskResultListener> listeners = new ArrayList<TaskResultListener>();

    private TaskRPC taskRPC;

    private class Task implements Runnable {
        private final Number160 taskId;

        private final Worker mapper;

        private final Map<Number160, Data> inputData;

        private final PeerAddress senderAddress;

        private final boolean sign;

        private final Peer peer;

        final Number320 taskKey;

        public Task(Peer peer, Number160 taskId, Worker mapper, Map<Number160, Data> inputData,
                PeerAddress senderAddress, boolean sign) {
            this.peer = peer;
            this.taskId = taskId;
            this.mapper = mapper;
            this.inputData = inputData;
            this.senderAddress = senderAddress;
            this.sign = sign;
            this.taskKey = new Number320(taskId, peer.getPeerID());
        }

        @Override
        public void run() {
            Thread.currentThread().setName("task-manager " + taskId);
            if (logger.isDebugEnabled()) {
                logger.debug("started task " + taskId + " which came from " + senderAddress);
            }

            synchronized (lock) {
                status.put(taskKey, Status.STARTED);
            }
            Map<Number160, Data> outputData = null;
            try {
                outputData = mapper.execute(peer, taskId, inputData);
            } catch (Exception e) {
                outputData = null;
                registerException(taskKey, e.toString());
            }
            synchronized (lock) {
                status.put(taskKey, Status.SUCCESS_RESULT_NOT_SENT);
            }
            final Map<Number160, Data> outputData2 = outputData;
            FutureChannelCreator futureChannelCreator = connectionBean.getConnectionReservation().reserve(1);
            futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
                @Override
                public void operationComplete(final FutureChannelCreator futureChannelCreator) throws Exception {
                    if (futureChannelCreator.isSuccess()) {
                        FutureResponse futureResponse = getTaskRPC().sendResult(senderAddress,
                                futureChannelCreator.getChannelCreator(), taskId, outputData2,
                                peer.getPeerBean().getKeyPair(), false, sign);
                        futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
                            @Override
                            public void operationComplete(FutureResponse future) throws Exception {
                                if (future.isSuccess()) {
                                    synchronized (lock) {
                                        status.put(taskKey, Status.SUCCESS_RESULT_SENT);
                                    }
                                } else {
                                    registerException(taskKey, "could not send result back");
                                }
                                connectionBean.getConnectionReservation().release(
                                        futureChannelCreator.getChannelCreator());
                            }
                        });
                    } else {
                        registerException(taskKey, "could not reserve connection");
                    }
                }
            });
        }
    }

    public TaskManager(ConnectionBean connectionBean, int threads) {
        this.connectionBean = connectionBean;
        this.executor = new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());
    }

    public void addListener(TaskResultListener taskResultListener) {
        listeners.add(taskResultListener);
    }

    public void removeListener(TaskResultListener taskResultListener) {
        listeners.remove(taskResultListener);
    }

    public void notifyListeners(Number320 taskKey, Map<Number160, Data> dataMap) {
        for (TaskResultListener taskResultListener : listeners) {
            taskResultListener.taskReceived(taskKey, dataMap);
        }
    }

    public void init(TaskRPC taskRPC) {
        this.taskRPC = taskRPC;
    }

    public TaskRPC getTaskRPC() {
        if (taskRPC == null) {
            throw new IllegalStateException("init() was not called yet");
        }
        return taskRPC;
    }

    public TaskStatus taskStatus(Number320 taskKey) {
        TaskStatus statusResult = new TaskStatus();
        String exception;
        synchronized (lock) {
            exception = exceptions.get(taskKey);
        }
        if (exception != null) {
            statusResult.setFaildeReason(exception);
            statusResult.setStatus(TaskStatus.Status.FAILED);
            if (logger.isDebugEnabled()) {
                logger.debug("finished task failed for task with ID " + taskKey);
            }
            return statusResult;
        }

        int pos = 0;
        Task taskFound = null;
        for (Runnable runnable : executor.getQueue()) {
            Task task = (Task) runnable;
            if (task.taskKey.equals(taskKey)) {
                taskFound = task;
                break;
            }
            pos++;
        }
        if (taskFound != null) {
            statusResult.setQueuePosition(pos);
            statusResult.setStatus(TaskStatus.Status.QUEUE);
            if (logger.isDebugEnabled()) {
                logger.debug("finished task queue for task with ID " + taskKey);
            }
            return statusResult;
        }
        synchronized (lock) {
            statusResult.setStatus(status.get(taskKey));
        }
        if (logger.isDebugEnabled()) {
            logger.debug("finished task status for task with ID " + taskKey);
        }
        return statusResult;
    }

    public int submitTask(Peer peer, Number160 taskId, Worker mapper, Map<Number160, Data> data,
            PeerAddress senderAddress, boolean sign) {
        final Number320 taskKey = new Number320(taskId, peer.getPeerID());
        synchronized (lock) {
            status.put(taskKey, TaskStatus.Status.QUEUE);
        }
        Task task = new Task(peer, taskId, mapper, data, senderAddress, sign);
        executor.execute(task);
        return executor.getQueue().size();
    }

    private void registerException(Number320 taskKey, String string) {
        synchronized (lock) {
            exceptions.put(taskKey, string);
        }
    }

    public DigestInfo digest() {
        return new DigestInfo(executor.getQueue().size());
    }

    public void shutdown() {
        List<Runnable> jobs = executor.shutdownNow();
        if (jobs.size() > 0 && logger.isWarnEnabled()) {
            logger.warn("shutting down and not executing " + jobs.size() + " jobs");
        }
    }
}