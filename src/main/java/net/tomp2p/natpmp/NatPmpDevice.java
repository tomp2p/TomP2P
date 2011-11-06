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

/**
 * This class manages a NAT-PMP device. This class is thread-safe.
 *
 * This manages communication with a NAT-PMP device on the network. There are
 * two types of messages that can be sent to the device.
 * {@link ExternalAddressRequestMessage} can be sent to get the external IP of
 * the gateway. {@link MapRequestMessage} can be sent to map a port for a
 * certain amount of time. These two messages can be put into the message queue
 * through the {@link #enqueueMessage(Message)} method.
 *
 * As this class manages a message queue to the NAT-PMP device, it is important
 * to shut it down correctly. Any mapped ports that are no longer desired should
 * be unmapped before shutdown occurs. Refer to {@link #NatPmpDevice(boolean)}
 * for details about the shutdown mechanism.
 * @author flszen
 */
public class NatPmpDevice {
    // Shutdown control instance fields.
    private boolean isShutdown = false;
    private final Object shutdownLock = new Object();

    private MessageQueue messageQueue;

    /**
     * Constructs a new NatPmpDevice.
     *
     * @param shutdownHookEnabled Shutting down existing port mappings is a
     * desired behavior; therefore, this value is required! Refer to
     * {@link #setShutdownHookEnabled(boolean)} for details about what value
     * should be provided here and how it alters the object's behavior.
     *
     * @throws NatPmpException A NatPmpException may be thrown if the local
     * network is not using addresses defined in RFC1918. A NatPmpException may
     * also be thrown if the the network gateway cannot be determined, which may
     * rarely be due to the network not using IPv4. NAT-PMP should only be used 
     * on RFC1918 networks using IPv4.
     *
     * @see #setShutdownHookEnabled(boolean)
     * @see #shutdown()
     */
    public NatPmpDevice(InetAddress gateway) throws NatPmpException {
        // Reject if the gateway is null.
        // This indicates either no gateway or the network is not IPv4.
        // It could also be that the netstat response is not supported.
        if (gateway == null) {
            throw new NatPmpException("The network gateway cannot be located.");
        }

        // Reject if it is not RFC1918.
        if (!gateway.isSiteLocalAddress()) {
            throw new NatPmpException("The network gateway address is not RFC1918 compliant.");
        }

        // Set up messaging queue.
        messageQueue = MessageQueue.createMessageQueue(gateway);
    }

    /**
     * Enqueues a message for sending.
     * @param message The {@link Message} to send.
     * @see #clearQueue()
     */
    public void enqueueMessage(Message message) {
        messageQueue.enqueueMessage(message);
    }
    
    /**
     * Clears the queue of messages to send. If a message is currently sending,
     * it is not interrupted.
     * @see #enqueueMessage(Message)
     */
    public void clearQueue() {
        messageQueue.clearQueue();
    }

    /**
     * Synchronously waits until the queue is empty before returning.
     */
    public void waitUntilQueueEmpty() {
        messageQueue.waitUntilQueueEmpty();
    }

    /**
     * Shuts down this NatPmpDevice. If the shutdown hook is disabled, this
     * method should be called manually at the time port mappings through the
     * NAT-PMP gateway are no longer needed. If the shutdown hook is enabled,
     * this method is called automatically during Java VM shutdown.
     *
     * When this method is called, if the shutdown hook is enabled, it is
     * automatically disabled.
     *
     * It should be noted that when this method is called manually, it blocks
     * until it completes. If it is desired to shutdown asynchronously, the
     * {@link #shutdownAsync(boolean)} method should be called.
     *
     * @see #isShutdown()
     * @see #isShutdownHookEnabled()
     * @see #setShutdownHookEnabled(boolean)
     * @see #shutdownAsync(boolean)
     */
    public void shutdown() {
        synchronized (shutdownLock) {
            // Do the shutdown stuff.
            messageQueue.shutdown();

            // Set the isShutdown flag.
            isShutdown = true;
        }
    }

    /**
     * Flag indicates if this NatPmpDevice is shutdown. This method will block
     * if a shutdown is in progress. If you desire to wait for an asynchronous
     * shutdown to complete, please monitor the returned {@link Thread} instead.
     * @return True if this NatPmpDeviceTest is shutdown, false if it is not.
     * @see #shutdown() 
     */
    public boolean isShutdown() {
        synchronized (shutdownLock) {
            return isShutdown;
        }
    }
}
