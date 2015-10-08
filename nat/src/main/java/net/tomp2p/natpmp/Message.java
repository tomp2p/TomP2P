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

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.PortUnreachableException;
import java.net.SocketException;
import java.net.SocketTimeoutException;

/**
 * The message class manages a message that is being sent to a NAT-PMP gateway.
 * The class respects the back-off time defined in the NAT-PMP specification.
 * Consumers of this class need to ensure that only one message is being sent at
 * a time to remain compliant with the NAT-PMP specification. This class is
 * thread-safe.
 * 
 * @see ExternalAddressRequestMessage
 * @see MapRequestMessage
 * @author flszen
 */
public abstract class Message {
    private MessageType type;

    private DatagramSocket socket;

    // These are used to manage the response.
    private byte[] response;

    private NatPmpException responseException;

    private MessageResponseInterface listener;

    // These are global response values.
    private ResultCode resultCode;

    private Integer secondsSinceEpoch;

    /**
     * Constructs a new Message with the specified type.
     * 
     * @param type
     *            The {@link MessageType} of the message. This must not be null.
     * @throws NullPointerException
     *             Thrown if MessageType is null.
     */
    Message(MessageType type, MessageResponseInterface listener) {
        // Localize the type. It is needed when a response is received.
        this.type = type;
        this.listener = listener;

        // Reject if MessageType is null.
        if (type == null) {
            throw new NullPointerException("MessageType must not be null.");
        }
    }

    /**
     * Gets a byte array initialized with the request packet payload. The
     * version and opcode fields are overwritten when the packet is sent,
     * therefore it is not necessary to initialize them in this method.
     * 
     * @return A byte arrray initialized with request packet payload.
     */
    abstract byte[] getRequestPayload();

    /**
     * Gets the opcode of this message.
     * 
     * @return The opcode.
     */
    abstract byte getOpcode();

    /**
     * Parses the response. This method is automatically called once after a
     * message is received.
     * 
     * @throws Throwable
     *             Any exception may be thrown by the implementer.
     */
    abstract void parseResponse(byte[] response) throws Exception;

    /**
     * Gets the exception associated with the response. This exception is
     * generally provided by an external entity.
     * 
     * @return The {@link NatPmpException} associated with the response.
     */
    public NatPmpException getResponseException() {
        return responseException;
    }

    /**
     * Gets the result code.
     * 
     * @return The result code.
     * @throws NatPmpException
     *             Thrown when no response has been received or the response
     *             parsing ran into some trouble.
     */
    public ResultCode getResultCode() throws NatPmpException {
        // Return the address.
        return resultCode;
    }

    /**
     * Gets the seconds since epoch as received from the NAT-PMP gateway.
     * 
     * @return The seconds since the epoch on the gateway.
     * @throws NatPmpException
     *             Thrown when no response has been received or the response
     *             parsing ran into some trouble.
     */
    public Integer getSecondsSinceEpoch() throws NatPmpException {
        // Return the address.
        return secondsSinceEpoch;
    }

    /**
     * Sets the exception associated with the response. This exception is
     * generally provided by an external entity.
     * 
     * @param responseException
     *            The {@link NatPmpException} to associate with the response.
     */
    private void setResponseException(NatPmpException responseException) {
        this.responseException = responseException;
    }

    /**
     * Parses the response and notifies the listener of the outcome.
     */
    void notifyListener() {
        // If there is no response, make that notification.
        if (response == null) {
            if (listener != null) {
                listener.noResponseReceived(this);
            }
            return;
        }

        // Run the internal method for this. It simplifies the logic in handling
        // exception cases.
        try {
            internalNotifyListener();
        } catch (NatPmpException ex) {
            setResponseException(ex);
        }

        // If an exception is present, report it, otherwise it's a good
        // response.
        if (getResponseException() != null) {
            if (listener != null) {
                listener.exceptionGenerated(this, getResponseException());
            }
        } else {
            // Make the notification that everyone hopes for.
            if (listener != null) {
                listener.responseReceived(this);
            }
        }
    }

    private void internalNotifyListener() throws NatPmpException {
        // Parse the global response values.
        secondsSinceEpoch = intFromByteArray(response, 4);
        switch (shortFromByteArray(response, 2)) {
        case 0:
            resultCode = ResultCode.Success;
            break;

        case 1:
            resultCode = ResultCode.UnsupportedVersion;
            break;

        case 2:
            resultCode = ResultCode.NotAuthorizedRefused;
            break;

        case 3:
            resultCode = ResultCode.NetworkFailure;
            break;

        case 4:
            resultCode = ResultCode.OutOfResources;
            break;

        case 5:
            resultCode = ResultCode.UnsupportedOpcode;
            break;

        default:
            throw new NatPmpException("Unsupported Result Code: " + shortFromByteArray(response, 2));
        }

        // Throw an exception if the result code is not success.
        if (resultCode != ResultCode.Success) {
            throw new NatPmpException("Message was not successful. The returned message code was "
                    + resultCode.toString() + ".");
        }

        // Verify the resultant opcode is correct.
        if (response[1] != getOpcode() + (byte) 128) {
            throw new NatPmpException("Incorrect opcode received from gateway.");
        }

        // Parse the response.
        try {
            parseResponse(response);
        } catch (Exception ex) {
            if (ex instanceof NatPmpException) {
                throw (NatPmpException) ex;
            } else {
                throw new NatPmpException("Exception encountered during parsing of response.", ex);
            }
        }
    }

    /**
     * Gets the {@link MessageType} of this Message.
     * 
     * @return The MessageType.
     */
    MessageType getMessageType() {
        return type;
    }

    /**
     * Parses an unsigned integer (long) from a byte array in network byte
     * order.
     * 
     * @param src
     *            The source array.
     * @param offset
     *            The offset in the array.
     * @return The parsed unsigned integer. (Represented as a long.)
     */
    static final int intFromByteArray(byte[] src, int offset) {
        return (int) ((src[offset] & 0xFF) << 24) + ((src[offset + 1] & 0xFF) << 16) + ((src[offset + 2] & 0xFF) << 8)
                + (src[offset + 3] & 0xFF);
    }

    /**
     * Writes a unsigned integer (long) to a byte array in network byte order.
     * 
     * @param value
     *            The value to write.
     * @param array
     *            The array to write to.
     * @param offset
     *            The offset in the array.
     */
    static final void intToByteArray(int value, byte[] array, int offset) {
        array[offset] = (byte) (value >>> 24);
        array[offset + 1] = (byte) (value >>> 16);
        array[offset + 2] = (byte) (value >>> 8);
        array[offset + 3] = (byte) value;
    }

    /**
     * Parses an unsigned short (int) from a byte array in network byte order.
     * 
     * @param src
     *            The source array.
     * @param offset
     *            The offset in the array.
     * @return The parsed unsigned short. (Represented as a long.)
     */
    static final int shortFromByteArray(byte[] src, int offset) {
        return (int) ((src[offset] & 0xFF) << 8) + (src[offset + 1] & 0xFF);
    }

    /**
     * Writes a unsigned short (int) to a byte array in network byte order.
     * 
     * @param value
     *            The value to write.
     * @param array
     *            The array to write to.
     * @param offset
     *            The offset in the array.
     */
    static final void shortToByteArray(int value, byte[] array, int offset) {
        array[offset] = (byte) (value >>> 8);
        array[offset + 1] = (byte) value;
    }

    /**
     * Sends the message. The sending process is synchronous. The NAT-PMP
     * protocol specifies back-off times that are respected by this
     * implementation. Each message object may only be sent once.
     * 
     * @param destination
     *            The destination address.
     * @throws NatPmpException
     *             Thrown when this message is already being sent or if there is
     *             a problem setting up the {@link DatagramSocket}.
     */
    synchronized void sendMessage(InetAddress destination) {
        try {
            sendMessageInternal(destination);
        } catch (NatPmpException ex) {
            setResponseException(ex);
        }
    }

    private void sendMessageInternal(InetAddress destination) throws NatPmpException {
        // Reject if a send is ongoing.
        if (this.socket != null) {
            throw new NatPmpException("Message is already being sent.");
        }

        // Set up the socket.
        try {
            socket = new DatagramSocket();
            socket.connect(destination, 5351);
            socket.setSoTimeout(250);
        } catch (IOException ex) {
            // Try to clean up the socket.
            if (socket != null) {
                socket.close();
                socket = null;
            }

            throw new NatPmpException("Exception during socket setup.", ex);
        }

        // Perform the sending and receiving process.
        sendMessageInternal();

        // Close the socket.
        socket.close();
    }

    /**
     * This handles the execution of the message sending and receiving process.
     */
    private void sendMessageInternal() throws NatPmpException {
        // Loop until all attempts have been made.
        for (int attempts = 4; attempts > 0; attempts--) {
            // Send the packet.
            try {
                byte[] payload = getRequestPayload();
                payload[0] = 0;
                payload[1] = getOpcode();
                DatagramPacket packet = new DatagramPacket(payload, payload.length, socket.getRemoteSocketAddress());
                socket.send(packet);
            } catch (PortUnreachableException ex) {
                throw new NatPmpException("The gateway is unreachable.");
            } catch (IOException ex) {
                throw new NatPmpException("Exception while sending packet.", ex);
            }

            // Wait for a response.
            try {
                byte[] localResponse = new byte[16];
                DatagramPacket packet = new DatagramPacket(localResponse, 0, 16);
                socket.receive(packet);

                // Handle a response.
                if (packet.getLength() > 0) {
                    this.response = localResponse;
                    return;
                }
            } catch (SocketTimeoutException ex) {
                // Ignore this.
            } catch (PortUnreachableException ex) {
                throw new NatPmpException("The gateway is unreachable.");
            } catch (IOException ex) {
                throw new NatPmpException("Exception while waiting for packet to be received.", ex);
            }

            // Double the socket timeout for the next attempt.
            try {
                socket.setSoTimeout(socket.getSoTimeout() * 2);
            } catch (SocketException ex) {
                throw new NatPmpException("Exception while increasing socket timeout time.", ex);
            }
        }
    }
}