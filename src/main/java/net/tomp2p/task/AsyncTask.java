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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.Scheduler;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.Cancellable;
import net.tomp2p.futures.FutureAsyncTask;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.TaskRPC;
import net.tomp2p.storage.Data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncTask implements TaskResultListener {
    final private static Logger logger = LoggerFactory.getLogger(AsyncTask.class);

    final private TaskRPC taskRPC;

    final private Scheduler scheduler;

    final private PeerBean peerBean;

    final private Map<Number320, FutureAsyncTask> tasks = new ConcurrentHashMap<Number320, FutureAsyncTask>();

    public AsyncTask(TaskRPC taskRPC, Scheduler scheduler, PeerBean peerBean) {
        this.taskRPC = taskRPC;
        this.scheduler = scheduler;
        this.peerBean = peerBean;
    }

    public FutureAsyncTask submit(final PeerAddress remotePeer, ChannelCreator channelCreator, final Number160 taskId,
            Map<Number160, Data> dataMap, Worker mapper, boolean forceUDP, boolean sign) {
        final Number320 taskKey = new Number320(taskId, remotePeer.getPeerId());
        final FutureAsyncTask futureAsyncTask = new FutureAsyncTask(remotePeer);
        futureAsyncTask.addCancellation(new Cancellable() {
            @Override
            public void cancel() {
                taskFailed(taskKey);
            }
        });
        tasks.put(taskKey, futureAsyncTask);
        FutureResponse futureResponse = taskRPC.sendTask(remotePeer, channelCreator, taskId, dataMap, mapper,
                peerBean.getKeyPair(), forceUDP, sign);
        futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
            @Override
            public void operationComplete(FutureResponse future) throws Exception {
                if (future.isSuccess()) {
                    // keep track of it and poll to see if its still alive
                    scheduler.keepTrack(remotePeer, taskId, (TaskResultListener) AsyncTask.this);
                } else {
                    futureAsyncTask.setFailed(future);
                }
            }
        });
        return futureAsyncTask;
    }

    @Override
    public void taskReceived(Number320 taskKey, Map<Number160, Data> dataMap) {
        if (logger.isDebugEnabled()) {
            logger.debug("Task received " + taskKey);
        }
        scheduler.stopKeepTrack(taskKey);
        FutureAsyncTask futureAsyncTask = tasks.remove(taskKey);
        if (futureAsyncTask == null) {
            logger.error("Task that was completed was not in the tracking list: " + taskKey);
            return;
        }
        futureAsyncTask.setDataMap(dataMap);
    }

    @Override
    public void taskFailed(Number320 taskKey) {
        if (logger.isDebugEnabled()) {
            logger.debug("Task failed " + taskKey);
        }
        // this comes from the scheduler
        FutureAsyncTask futureAsyncTask = tasks.remove(taskKey);
        if (futureAsyncTask == null) {
            return;
        }
        futureAsyncTask.setFailed("polling faild, maybe peer died");
    }
}