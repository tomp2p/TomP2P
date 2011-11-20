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
/**
 * Represents the result of an asynchronous operation.
 * 
 * @author Thomas Bocek
 */
public interface BaseFuture extends Cancellable
{
	/**
	 * The first state is always INIT and will always end in either OK, FAILED,
	 * or CANCEl
	 */
	public enum FutureType
	{
		INIT, OK, FAILED, CANCEL
	};

	/**
	 * Wait for the asynchronous operation to end.
	 * 
	 * @return this
	 * @throws InterruptedException if thread is interrupted
	 */
	public abstract BaseFuture await() throws InterruptedException;

	/**
	 * Wait for the asynchronous operation to end with the specified timeout.
	 * 
	 * @param timeoutMillis time to wait at most
	 * @return true if the operation is finished.
	 * @throws InterruptedException if thread is interrupted
	 */
	public abstract boolean await(long timeoutMillis) throws InterruptedException;

	/**
	 * Wait for the asynchronous operation to end without interruption.
	 * 
	 * @return this
	 */
	public abstract BaseFuture awaitUninterruptibly();

	/**
	 * Wait for the asynchronous operation to end with the specified timeout
	 * without interruption.
	 * 
	 * @param me to wait at most
	 * @return true if the operation is finished.
	 */
	public abstract boolean awaitUninterruptibly(long timeoutMillis);

	/**
	 * Checks if the asynchronous operation is finished.
	 * 
	 * @return true if operation is finished
	 */
	public abstract boolean isCompleted();

	/**
	 * Returns the opposite of isFailed (returns !isFailed). Use this method if
	 * you are an optimist ;) otherwise use isFailed
	 * 
	 * @return true if operation succeeded, false if there was no reply
	 */
	public abstract boolean isSuccess();

	/**
	 * Checks if operation has failed. As this is a P2P network, where peers can
	 * fail at any time, a failure is seen as a "normal" event. Thus, no
	 * exception is thrown.
	 * 
	 * @return true if operation failed, which means the node did not reply. A
	 *         get(key) operation on a node that does not have the key, returns
	 *         false with this method as a response has been send.
	 */
	public abstract boolean isFailed();

	/**
	 * Sets the failed flat to true and the completed flag to true. This will
	 * notify listeners and set the reason
	 */
	public abstract void setFailed(String reason);

	/**
	 * The default failed reason is Unknown.
	 * 
	 * @return Returns the reason why a future failed.
	 */
	public abstract String getFailedReason();

	/**
	 * If the type is not OK, then something unexpected happened.
	 * 
	 * @return The fail type
	 */
	public abstract FutureType getType();

	/**
	 * Adds a listener which is notified when the state of this future changes.
	 * All notifications are performed in a thread, which means that this method
	 * returns immediately. If a future is complete, then all listeners are
	 * called and after that, the listener list is cleared, so there is no need
	 * to call removeListener if a future has been completed.
	 * 
	 * @param listener The listener extends the BaseFuture
	 * @return this
	 */
	public abstract BaseFuture addListener(BaseFutureListener<? extends BaseFuture> listener);

	/**
	 * Removes a listener which is notified when the state of this future
	 * changes. If a future is complete, then all listeners are called and after
	 * that, the listener list is cleared, so there is no need to call
	 * removeListener if a future has been completed. The listener can be called
	 * from the caller thread if the future is already finished or from a
	 * different thread if the future is not ready yet.
	 * 
	 * @param listener The listener extends the BaseFuture
	 * @return this
	 */
	public abstract BaseFuture removeListener(BaseFutureListener<? extends BaseFuture> listener);

	/**
	 * Adds a cancel listener to this future, which is called when cancel is
	 * executed. There is no need to call removeCancellation if a future has
	 * been completed, because the cancellable list is cleared after the future
	 * has been completed.
	 * 
	 * An example usage for a cancelation is if a TCP connection is being
	 * created, but the user shuts down the peer.
	 * 
	 * @param cancellable A cancellable class
	 * @param addIfRunning True: only add the cancelation if the future is still running 
	 */
	public abstract BaseFuture addCancellation(Cancellable cancellable, boolean addIfRunning);

	/**
	 * Remove a listener. After a future is completed, all cancellables are
	 * removed. There is no need to call removeCancellation if a future has been
	 * completed, because the cancellable list is cleared after the future has
	 * been completed anyway.
	 * 
	 * @param cancellable A cancellable class
	 */
	public abstract BaseFuture removeCancellation(Cancellable cancellable);
}