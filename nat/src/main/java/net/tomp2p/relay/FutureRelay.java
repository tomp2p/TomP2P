package net.tomp2p.relay;

import java.util.Collection;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.BaseFutureImpl;

public class FutureRelay extends BaseFutureImpl<FutureRelay> {

	private Collection<PeerConnection> relays;

	private boolean isRelayNotRequired;

	public FutureRelay() {
		self(this);
	}

	public FutureRelay nothingTodo() {
		synchronized (lock) {
			if (!completedAndNotify()) {
				return this;
			}
			type = FutureType.OK;
			isRelayNotRequired = true;
		}
		notifyListeners();
		return this;
	}

	public FutureRelay done(Collection<PeerConnection> relays) {
		synchronized (lock) {
			if (!completedAndNotify()) {
				return this;
			}
			type = FutureType.OK;
			this.relays = relays;
		}
		notifyListeners();
		return this;
	}

	public boolean isRelayNotRequired() {
		synchronized (lock) {
			return isRelayNotRequired;
		}
	}

	public Collection<PeerConnection> relays() {
		synchronized (lock) {
			return relays;
		}
	}
}
