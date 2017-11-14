package net.tomp2p.connection;

import java.io.IOException;

public interface ClientChannel {
	void close() throws IOException;
}
