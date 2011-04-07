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
 * Each response has at least a request messages. The corresponding response
 * message is set only if the request has been successful. This is indicated
 * with the failed field.
 * 
 * @author draft
 * 
 */
public class FutureResponse extends BaseFutureImpl
{
	final private Message requestMessage;
	private Message responseMessage;
	private ReplyTimeoutHandler replyTimeoutHandler;
	private long replyTimeoutMillis=Long.MAX_VALUE;

	public FutureResponse(final Message requestMessage)
	{
		this.requestMessage = requestMessage;
	}
	
	public void setResponse()
	{
		setResponse(null);
	}

	/**
	 * Gets called if a peer responds. Note that either this method or
	 * responseFailed() is always called.
	 * 
	 * @param message The received message
	 */
	public void setResponse(final Message responseMessage)
	{
		synchronized (lock)
		{
			if (!setCompletedAndNotify())
				return;
			if (responseMessage != null)
			{
				this.responseMessage = responseMessage;
				type = (responseMessage.getType() == Message.Type.OK)
						|| (responseMessage.getType() == Message.Type.PARTIALLY_OK) ? FutureType.OK
						: FutureType.FAILED;
				reason = responseMessage.getType().toString();
			}
			else
			{
				type = FutureType.OK;
				reason = "Message delivered";
			}
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

	public void setReplyTimeoutHandler(final ReplyTimeoutHandler replyTimeoutHandler)
	{
		synchronized (lock)
		{
			this.replyTimeoutHandler = replyTimeoutHandler;
		}
	}

	public void cancelTimeout()
	{
		synchronized (lock)
		{
			this.replyTimeoutHandler.cancel();
		}
	}

	public void setReplyTimeout(long replyTimeoutMillis)
	{
		synchronized (lock)
		{
			this.replyTimeoutMillis=replyTimeoutMillis;
		}
	}

	public long getReplyTimeout()
	{
		synchronized (lock)
		{
			return replyTimeoutMillis;
		}
	}
}
