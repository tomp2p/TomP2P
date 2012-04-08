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
import net.tomp2p.connection.ReplyTimeoutHandler;
import net.tomp2p.message.Message;

/**
 * Each response has one request messages. The corresponding response message is
 * set only if the request has been successful. This is indicated with isFailed.
 * 
 * @author Thomas Bocek
 * 
 */
public class FutureResponse extends BaseFutureImpl
{
	// the message that was requested
	final private Message requestMessage;
	// the reply to this request
	private Message responseMessage;

	/**
	 * Create the future and set the request message
	 * 
	 * @param requestMessage The request message that will be send over the
	 *        wire.
	 */
	public FutureResponse(final Message requestMessage)
	{
		this.requestMessage = requestMessage;
	}

	/**
	 * If we don't get a reply message, which is the case for fire-and-forget
	 * messages, then set the reply to null and set this future to complete with
	 * the type Success.
	 */
	public void setResponse()
	{
		setResponse(null);
	}

	/**
	 * Gets called if a peer responds. Note that either this method or
	 * responseFailed() is always called. This does not notify any listeners.
	 * The listeners gets notified if channel is closed
	 * 
	 * @param message The received message
	 */
	public void setResponse(final Message responseMessage)
	{
		synchronized (lock)
		{
			if (!setCompletedAndNotify())
			{
				return;
			}
			if (responseMessage != null)
			{
				this.responseMessage = responseMessage;
				//if its ok or nok, the communication was successful. Everything else is a failure in communication
				type = (responseMessage.isOk() || responseMessage.isNotOk()) ? FutureType.OK
						: FutureType.FAILED;
				reason = responseMessage.getType().toString();
			}
			else
			{
				type = FutureType.OK;
				reason = "Nothing to deliver...";
			}
		}
		notifyListerenrs();
	}

	@Override
	public void setFailed(String reason)
	{
		synchronized (lock)
		{
			if (!setCompletedAndNotify())
				return;
			this.reason = reason;
			this.type = FutureType.FAILED;
		}
		notifyListerenrs();
	}

	/**
	 * Returns the response message. This is the same message as in
	 * response(Message message). If no response where send, then this will
	 * return null.
	 * 
	 * @return The successful response message or null if failed
	 */
	public Message getResponse()
	{
		synchronized (lock)
		{
			return responseMessage;
		}
	}

	/**
	 * The future response always keeps a reference to the request.
	 * 
	 * @return The request message.
	 */
	public Message getRequest()
	{
		synchronized (lock)
		{
			return requestMessage;
		}
	}

	/**
	 * Set the cancel operation for the timeout handler.
	 * 
	 * @param replyTimeoutHandler The timeout that needs to be canceled if the
	 *        future returns successfully.
	 */
	public void setReplyTimeoutHandler(final ReplyTimeoutHandler replyTimeoutHandler)
	{
		addListener(new BaseFutureAdapter<FutureResponse>()
		{
			@Override
			public void operationComplete(FutureResponse future) throws Exception
			{
				replyTimeoutHandler.cancel();
			}
		});
	}
}
