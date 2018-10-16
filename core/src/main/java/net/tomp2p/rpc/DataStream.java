package net.tomp2p.rpc;

import net.tomp2p.connection.DataSend;

import java.nio.ByteBuffer;

public interface DataStream {
	ByteBuffer receiveSend(ByteBuffer buffer, DataSend dataSend);
}
