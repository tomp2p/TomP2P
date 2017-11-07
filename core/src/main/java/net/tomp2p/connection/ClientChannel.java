package net.tomp2p.connection;

import java.io.IOException;

public interface ClientChannel {
	ClientChannel close() throws IOException;
}
