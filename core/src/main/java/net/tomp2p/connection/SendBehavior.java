package net.tomp2p.connection;

import net.tomp2p.message.Message;
import net.tomp2p.peers.PeerAddress;

/**
 * Decice how a direct message is sent.
 * 
 * @author Nico Rutishauser
 * @author Thomas Bocek
 * 
 */
public interface SendBehavior {

	public enum SendMethod {
                EXISTING_CONNECTION,
		/**
		 * Send the message directly to the receiver
		 */
		DIRECT,

		/**
		 * Send the message after a direct connection via hole punching is
		 * established. If it fails, use a relay
                 * Send the message to the relay which forwards it to the receiver
		 */
		HOLEP_RELAY,

		/**
		 * Open a reverse connection to the receiver and send the message. The
		 * reverse connection is closed afterwards.
		 */
		RCON,

		/**
		 * Don't send the message over the network but directly pass it to the
		 * own dispatcher
		 */
		SELF,
	}

	/**
	 * Returns the send behavior depending on the message to be sent over TCP.
	 * 
	 * @param message
	 *            the message to be sent
	 * @return the sending behavior which should be used
	 */
	SendMethod tcpSendBehavior(Dispatcher dispatcher, PeerAddress sender, 
                PeerAddress recipient, boolean isReflected);

	/**
	 * Returns the send behavior depending on the message to be sent over UDP.
	 * 
	 * @param message
	 *            the message to be sent
	 * @return the sending behavior which should be used
	 * @throws UnsupportedOperationException
	 *             sending over UDP is not allowed for this message.
	 */
	SendMethod udpSendBehavior(Dispatcher dispatcher, PeerAddress sender, 
                PeerAddress recipient, boolean isReflected);
}
