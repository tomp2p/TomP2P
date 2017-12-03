package net.tomp2p.connection;

import java.io.IOException;

import net.sctp4nat.core.SctpChannel;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Message;

public interface ClientChannel {
	void close() throws IOException;

	//send SCTP
	SctpChannel channel();
	
	//send TomP2P message
	void send(final Message message, final FutureDone<Message> futureMessage);
}
