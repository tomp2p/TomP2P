package net.tomp2p.connection;

import net.tomp2p.message.Message;
import net.tomp2p.rpc.RPC;

/**
 * Default sending behavior for UDP and TCP messages. Depending whether the recipient is relayed, slow and on
 * the message size, decisions can be made here.
 * 
 * @author Nico Rutishauser
 *
 */
public class DefaultSendBehavior implements SendBehavior {

	@Override
	public SendMethod tcpSendBehavior(Message message) {
		if (message.recipient().isRelayed()) {
			// TODO check the message size. If > 1500bytes, use RCON, otherwise use Relay peer
			if (message.sender().isRelayed()) {
				// reverse connection is not possible
				return SendMethod.RELAY;
			} else {
				return SendMethod.RCON;
			}
		} else {
			// send directly
			return SendMethod.DIRECT;
		}
	}

	@Override
	public SendMethod udpSendBehavior(Message message) throws UnsupportedOperationException {
		if (message.recipient().isRelayed()) {
			if (message.command() == RPC.Commands.NEIGHBOR.getNr() || message.command() == RPC.Commands.PING.getNr()) {
				return SendMethod.RELAY;
			} else {
				throw new UnsupportedOperationException(
						"Tried to send UDP message to unreachable peers. Only TCP messages can be sent to unreachable peers");
			}
		} else {
			return SendMethod.DIRECT;
		}
	}
}
