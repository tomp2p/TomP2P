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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.CountDownLatch;

import net.tomp2p.connection.ProgressHandler;
import net.tomp2p.message.Message;

/**
 * Each response has one request message. The corresponding response message is set only if the request has been
 * successful. This is indicated with isFailed.
 * 
 * @author Thomas Bocek
 */
public class FutureResponse extends BaseFutureImpl<FutureResponse> {
    // the message that was requested
    private final Message requestMessage;

    private final FutureSuccessEvaluator futureSuccessEvaluator;

    // the reply to this request
    private Message responseMessage;

    private ProgressHandler progressHandler;

    private final ProgressListener progressListener;

    private final CountDownLatch firstProgressHandler = new CountDownLatch(1);
    private final CountDownLatch secondProgressHandler = new CountDownLatch(1);
    
    private boolean reponseLater = false;

    /**
     * Creates a future and sets the request message.
     * 
     * @param requestMessage
     *            The request message that will be sent.
     */
    public FutureResponse(final Message requestMessage) {
        this(requestMessage, new FutureSuccessEvaluatorCommunication());
    }

    /**
     * Creates a future and sets the request message.
     * 
     * @param requestMessage
     *            The request message that will be sent.
     * @param futureSuccessEvaluator
     *            Evaluates if the future was a success or a failure.
     */
    public FutureResponse(final Message requestMessage, final FutureSuccessEvaluator futureSuccessEvaluator) {
        this(requestMessage, futureSuccessEvaluator, null);
    }

    /**
     * Creates a future and sets the request message. This will set the progress listener for streaming support.
     * 
     * @param requestMessage
     *            The request message that will be sent.
     * @param progressListener
     *            The progress listener for streaming support
     */
    public FutureResponse(final Message requestMessage, final ProgressListener progressListener) {
        this(requestMessage, new FutureSuccessEvaluatorCommunication(), progressListener);
    }

    /**
     * Create a future and sets the request message. This will set the progress listener for streaming support.
     * 
     * @param requestMessage
     *            The request message that will be sent.
     * @param futureSuccessEvaluator
     *            Evaluates if the future was a success or a failure.
     * @param progressListener
     *            The progress listener for streaming support
     */
    public FutureResponse(final Message requestMessage, final FutureSuccessEvaluator futureSuccessEvaluator,
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
    public FutureResponse emptyResponse() {
        return response(null);
    }

    /**
     * Gets called if a peer responds. Note that either this method or responseFailed() is always called. This does not
     * notify any listeners. The listeners gets notified if channel is closed
     * 
     * @param responseMessage
     *            The received message
     * @return This class
     */
    public FutureResponse response(final Message responseMessage) {
        synchronized (lock) {
            if (!completedAndNotify()) {
                return this;
            }
            if (responseMessage != null) {
                this.responseMessage = responseMessage;
                // if its ok or nok, the communication was successful.
                // Everything else is a failure in communication
                type = futureSuccessEvaluator.evaluate(requestMessage, responseMessage);
                reason = responseMessage.type().toString();
            } else {
                type = FutureType.OK;
                reason = "Nothing to deliver...";
            }
        }
        notifyListeners();
        return this;
    }

    public boolean responseLater(final Message responseMessage) {
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
                reason = responseMessage.type().toString();
            } else {
                type = FutureType.OK;
                reason = "Nothing to deliver...";
            }
        }
        return true;
    }
    
    public boolean failedLater(final Throwable cause) {
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
    
    public FutureResponse responseNow() {
        synchronized (lock) {
        	if (!reponseLater && !completed) {
        		failed("No future set beforehand, probably an early shutdown / timeout, or use setFailedLater() or setResponseLater()");
        		return this;
        	}
            if (!super.completedAndNotify()) {
                return this;
            }
        }
        notifyListeners();
        return this; 
    }
    
    protected boolean completedAndNotify() {
        if (reponseLater) {
            return false;
        }
        return super.completedAndNotify();
    }
    
    

    /**
     * Returns the response message. This is the same message as in response(Message message). If no response where
     * send, then this will return null.
     * 
     * @return The successful response message or null if failed
     */
    public Message responseMessage() {
        synchronized (lock) {
            return responseMessage;
        }
    }

    /**
     * The future response always keeps a reference to the request.
     * 
     * @return The request message.
     */
    public Message request() {
        synchronized (lock) {
            return requestMessage;
        }
    }

    /**
     * Set the user based progress handler, where the user can add more data and call {@link #progress()} when data has
     * been added.
     * 
     * @param progressHandler
     *            The progress handler that will be added by the TomP2P library.
     * @return This class
     */
    public FutureResponse progressHandler(final ProgressHandler progressHandler) {
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
            progressHandler.progress();
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
            progressHandler.progress();
        }
        return this;
    }

    /**
     * This will be called by the TomP2P library when a partial message is ready.
     * 
     * @param interMediateMessage
     *            The message that is either incomplete as indicated with {@link Message#isDone()} or completed. This
     *            will be called before the future completes.
     */
    public void progress(final Message interMediateMessage) {
        synchronized (lock) {
            if (progressListener != null) {
                progressListener.progress(interMediateMessage);
            }
        }
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("future response state:");
        sb.append(",type:").append(type.name()).append(",msg:").append(requestMessage.command()).append(",reason:").append(reason);
        return sb.toString();
    }
}
