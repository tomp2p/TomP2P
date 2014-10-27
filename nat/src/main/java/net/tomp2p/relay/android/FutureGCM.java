package net.tomp2p.relay.android;

import java.util.List;

import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Message;

public class FutureGCM extends FutureDone<Void> {

	private final List<Message> buffer;

	public FutureGCM(List<Message> buffer) {
		 self(this);
		 this.buffer = buffer;
	}
	
	public List<Message> buffer() {
		return buffer;
	}
}
