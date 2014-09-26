package net.tomp2p.relay.android;

import java.util.List;

import net.tomp2p.message.Buffer;

public interface BufferFullListener {

	void bufferFull(List<Buffer> buffer);
}
