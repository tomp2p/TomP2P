package net.tomp2p.connection;

import java.io.IOException;

import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Message;
import net.tomp2p.network.KCP;

public interface ClientChannel {
	void close() throws IOException;

	//send SCTP
	KCP channel();
	
	//send TomP2P message
	void send(final Message message, final FutureDone<Message> futureMessage);
}
