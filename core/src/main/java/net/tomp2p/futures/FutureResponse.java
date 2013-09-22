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
package net.tomp2p.futures;

import io.netty.channel.ChannelFuture;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.CountDownLatch;

import net.tomp2p.connection2.ProgresHandler;
import net.tomp2p.message.Message2;

/**
 * Each response has one request messages. The corresponding response message is set only if the request has been
 * successful. This is indicated with isFailed.
 * 
 * @author Thomas Bocek
 */
public class FutureResponse extends BaseFutureImpl<FutureResponse> {
    // the message that was requested
    private final Message2 requestMessage;

    private final FutureSuccessEvaluator futureSuccessEvaluator;

    // the reply to this request
    private Message2 responseMessage;

    private ProgresHandler progressHandler;

    private final ProgressListener progressListener;

    private final CountDownLatch firstProgressHandler = new CountDownLatch(1);
    private final CountDownLatch secondProgressHandler = new CountDownLatch(1);
    
    private ChannelFuture channelFuture;
    
    private boolean reponseLater = false;

    /**
     * Create the future and set the request message.
     * 
     * @param requestMessage
     *            The request message that will be send over the wire.
     */
    public FutureResponse(final Message2 requestMessage) {
        this(requestMessage, new FutureSuccessEvaluatorCommunication());
    }

    /**
     * Create the future and set the request message.
     * 
     * @param requestMessage
     *            The request message that will be send over the wire.
     * @param futureSuccessEvaluator
     *            Evaluates if the future was a success or failure
     */
    public FutureResponse(final Message2 requestMessage, final FutureSuccessEvaluator futureSuccessEvaluator) {
        this(requestMessage, futureSuccessEvaluator, null);
    }

    /**
     * Create the future and set the request message. This will set the progress listener for streaming support.
     * 
     * @param requestMessage
     *            The request message that will be send over the wire.
     * @param progressListener
     *            The progress listener for streaming support
     */
    public FutureResponse(final Message2 requestMessage, final ProgressListener progressListener) {
        this(requestMessage, new FutureSuccessEvaluatorCommunication(), progressListener);
    }

    /**
     * Create the future and set the request message. This will set the progress listener for streaming support.
     * 
     * @param requestMessage
     *            The request message that will be send over the wire.
     * @param futureSuccessEvaluator
     *            Evaluates if the future was a success or failure
     * @param progressListener
     *            The progress listener for streaming support
     */
    public FutureResponse(final Message2 requestMessage, final FutureSuccessEvaluator futureSuccessEvaluator,
            final ProgressListener progressListener) {
        this.requestMessage = requestMessage;
        this.futureSuccessEvaluator = futureSuccessEvaluator;
        this.progressListener = progressListener;
        self(this);
    }

    /**
     * If we don't get a reply message, which is the case for fire-and-forget messages, then set the reply to null and
     * set this future to complete with the type Success.
     * 
     * @return This class
     */
    public FutureResponse setResponse() {
        return setResponse(null);
    }

    /**
     * Gets called if a peer responds. Note that either this method or responseFailed() is always called. This does not
     * notify any listeners. The listeners gets notified if channel is closed
     * 
     * @param responseMessage
     *            The received message
     * @return This class
     */
    public FutureResponse setResponse(final Message2 responseMessage) {
        synchronized (lock) {
            if (!setCompletedAndNotify()) {
                return this;
            }
            if (responseMessage != null) {
                this.responseMessage = responseMessage;
                // if its ok or nok, the communication was successful.
                // Everything else is a failure in communication
                type = futureSuccessEvaluator.evaluate(requestMessage, responseMessage);
                reason = responseMessage.getType().toString();
            } else {
                type = FutureType.OK;
                reason = "Nothing to deliver...";
            }
        }
        notifyListerenrs();
        return this;
    }

    public boolean setResponseLater(final Message2 responseMessage) {
        //System.err.println("response later "+this);
        synchronized (lock) {
            if(completed) {
                return false;
            }
            reponseLater = true;
            if (responseMessage != null) {
                this.responseMessage = responseMessage;
                // if its ok or nok, the communication was successful.
                // Everything else is a failure in communication
                type = futureSuccessEvaluator.evaluate(requestMessage, responseMessage);
                reason = responseMessage.getType().toString();
            } else {
                type = FutureType.OK;
                reason = "Nothing to deliver...";
            }
        }
        return true;
    }
    
    public boolean setFailedLater(final Throwable cause) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        cause.printStackTrace(printWriter);
        synchronized (lock) {
            if(completed) {
                return false;
            }
            reponseLater = true;
            this.reason = stringWriter.toString();
            this.type = FutureType.FAILED;
        }
        return true;
    }
    
    public FutureResponse setResponseNow() {
        synchronized (lock) {
            if (!super.setCompletedAndNotify()) {
                return this;
            }
        }
        notifyListerenrs();
        return this; 
    }
    
    protected boolean setCompletedAndNotify() {
        if (reponseLater) {
            return false;
        }
        return super.setCompletedAndNotify();
    }
    
    

    /**
     * Returns the response message. This is the same message as in response(Message message). If no response where
     * send, then this will return null.
     * 
     * @return The successful response message or null if failed
     */
    public Message2 getResponse() {
        synchronized (lock) {
            return responseMessage;
        }
    }

    /**
     * The future response always keeps a reference to the request.
     * 
     * @return The request message.
     */
    public Message2 getRequest() {
        synchronized (lock) {
            return requestMessage;
        }
    }

    /**
     * Set the user based progres handler, where the user can add more data and call {@link #progress()} when data has
     * been added.
     * 
     * @param progressHandler
     *            The progress handler that will be added by the TomP2P library.
     * @return This class
     */
    public FutureResponse setProgressHandler(final ProgresHandler progressHandler) {
        synchronized (lock) {
            this.progressHandler = progressHandler;
        }
        firstProgressHandler.countDown();
        return this;
    }

    /**
     * Do not call this directly, as this is called from the TomP2P library. This must be the first call to the
     * progress handler, otherwise we might not be able to decode the header.
     * 
     * @return This class
     * @throws InterruptedException .
     */
    public FutureResponse progressFirst() throws InterruptedException {
        firstProgressHandler.await();
        synchronized (lock) {
            progressHandler.progres();
        }
        secondProgressHandler.countDown();
        return this;
    }

    /**
     * This will be called by the user, when the user provides more data. This will block until a progress handler has
     * been set by the TomP2P library.
     * 
     * @return This class
     * @throws InterruptedException
     *             If latch is interrupted
     */
    public FutureResponse progress() throws InterruptedException {
        secondProgressHandler.await();
        synchronized (lock) {
            progressHandler.progres();
        }
        return this;
    }

    /**
     * This will be called by the TomP2P library when a partial message is ready.
     * 
     * @param interMediateMessage
     *            The message that is either incomplete as indicated with {@link Message2#isDone()} or completed. This
     *            will be called before the future completes.
     */
    public void progress(final Message2 interMediateMessage) {
        synchronized (lock) {
            if (progressListener != null) {
                progressListener.progress(interMediateMessage);
            }
        }
    }

    public void setChannelFuture(ChannelFuture channelFuture) {
        synchronized (lock) {
           this.channelFuture = channelFuture;
        }
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("future response state:");
        sb.append(",type:").append(type.name()).append(",msg:").append(requestMessage.getCommand()).append(",reason:").append(reason).append(",ch.future:").append(channelFuture);
        return sb.toString();
    }
}
