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

import net.tomp2p.message.Message;
import net.tomp2p.peers.RTT;

/**
 * Each response has one request messages. The corresponding response message is set only if the request has been
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
    
    private boolean reponseLater = false;

    private final RTT roundTripTime = new RTT();

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
        this.requestMessage = requestMessage;
        this.futureSuccessEvaluator = futureSuccessEvaluator;
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
     * Start measuring time until reply is recieved
     *
     * @return True if time has successfully been started
     *         False if measurement already has been stopped
     */
    public boolean startRTTMeasurement(boolean isUDP) {
        return getRoundTripTime().beginTimeMeasurement(isUDP);
    }

    /**
     * Stops time measurement for the round trip time
     *
     * @return True if some valid time was recorded, False if time has not yet
     *         been started or already been stopped
     */
    public boolean stopRTTMeasurement() {
        return getRoundTripTime().stopTimeMeasurement();
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("future response state:");
        sb.append(",type:").append(type.name()).append(",msg:").append(requestMessage.command()).append(",reason:").append(reason);
        return sb.toString();
    }

    public RTT getRoundTripTime() {
        return roundTripTime;
    }
}
