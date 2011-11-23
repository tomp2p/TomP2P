/*
 * This file is part of jNAT-PMPlib.
 *
 * jNAT-PMPlib is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * jNAT-PMPlib is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with jNAT-PMPlib.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.tomp2p.natpmp;

import java.net.InetAddress;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MessageQueue manages a queue of {@link Message}s and ensures that only one
 * message at time is sent to the NAT-PMP device.
 *
 * To use the MessageQueue, simply call {@link createMesageQueue()}. This will
 * create and return a MessageQueue.
 *
 * {@link shutdown()} must be called when the queue is no longer needed.
 *
 * This class is thread-safe.
 * @author flszen
 */
class MessageQueue implements Runnable {
    private InetAddress gatewayIP;
    private Thread thread;
    private LinkedList<Message> queue;
    private final Object queueLock = new Object();
    private final Object messageLock = new Object();
    private boolean shutdown = false;
    private final Object shutdownLock = new Object();
    
    /**
     * Constructs a MessageQueue.
     */
    private MessageQueue(InetAddress gatewayIP) {
        // Localize.
        this.gatewayIP = gatewayIP;

        // Prepare the thread.
        thread = new Thread(this, "MessageQueue");
        thread.setDaemon(false);

        // Prepare the queue.
        queue = new LinkedList<Message>();
    }

    /**
     * Creates and starts a {@link MessageQueue}. The created MesageQueue is
     * returned. To use the queue, add {@link Message}s through the
     * {@link enqueueMessage(Message)} method.
     * @return The creates MessageQueue.
     */
    static MessageQueue createMessageQueue(InetAddress gatewayIP) {
        // Create and start the queue.
        MessageQueue messageQueue = new MessageQueue(gatewayIP);
        messageQueue.thread.start();
        
        // Allow the thread that we just started to run to its wait().
        while (messageQueue.thread.getState() != Thread.State.TIMED_WAITING) {
            Thread.yield();
        }

        // Return the queue.
        return messageQueue;
    }

    /**
     * Enqueues a {@link Message} in the queue.
     * @param message The {@link Message} to enqueue.
     */
    void enqueueMessage(Message message) {
        synchronized (queueLock) {
            queue.add(message);
            queueLock.notify();
        }
    }

    /**
     * Clears all messages in the queue. If a message is currently being
     * processed, it is not interrupted.
     */
    void clearQueue() {
        synchronized (queueLock) {
            queue.clear();
            queueLock.notify();
        }
    }

    /**
     * Shuts down this messageQueue. This method is synchronous. The queue is
     * cleared and it waits for the last item to be processed.
     */
    void shutdown() {
        // Signal shutdown.
        synchronized (shutdownLock) {
            shutdown = true;
        }

        // Clear the queue.
        clearQueue();

        // Wait for the shutdown to complete.
        while (thread.isAlive()) {
            try {
                Thread.sleep(25);
            } catch (InterruptedException ex) {
                Logger.getLogger(MessageQueue.class.getName()).log(Level.SEVERE, null, ex);
                break;
            }
        }
    }

    /**
     * Flag indicates that this MessageQueue is shutdown.
     * @return True if the MessageQueue is shut down, false if it is not.
     */
    boolean isShutdown() {
        synchronized (shutdownLock) {
            return shutdown;
        }
    }

    /**
     * Causes the current thread to wait until the queue is empty.
     */
    void waitUntilQueueEmpty() {
        int size = 0;
        synchronized (messageLock) {
            size = queue.size();

            // Wait in quarter-second intervals until the size is zero.
            while (size > 0) {
                // Release the messageLock for a 1/4 second. This prevents
                // deadlock with run().
                try {
                    messageLock.wait(250);
                } catch (InterruptedException ex) {
                    Logger.getLogger(MessageQueue.class.getName()).log(Level.SEVERE, null, ex);
                }
                size = queue.size();
            }
        }
    }

    public void run() {
        // Loop while running.
        while (!shutdown) {
            try {
                // Get a Message from the queue, if one is available.
                Message message = null;
                synchronized (messageLock) {
                    synchronized (queueLock) {
                        // Loop until a message is received.
                        while (message == null && !shutdown) {
                            if (queue.size() > 0) {
                                // Get the message to send.
                                message = queue.removeFirst();
                            } else {
                                // Wait for a message for up to 1/4 second.
                                queueLock.wait(250);

                                // Release messageLock briefly. This prevents
                                // deadlock during waitUntilQueueEmpty().
                                messageLock.wait(1);
                            }
                        }
                    }

                    // If shutdown is true, we may end up here. Continue the loop.
                    if (shutdown) {
                        continue;
                    }

                    // Send the message outside of the queueLock context.
                    message.sendMessage(gatewayIP);

                    // Notify the listener about the repsonse.
                    message.notifyListener();
                }
            } catch (InterruptedException ex) {
                // If the thread is interrupted, it is silently dropped.
                // Interruptions should not be received with this arrangement
                // anyway.
            }
        }
    }
}
