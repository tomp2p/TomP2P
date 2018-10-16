package net.tomp2p.connection;

import java.nio.ByteBuffer;

public interface DataSend {
    void send(ByteBuffer buffer);
}
