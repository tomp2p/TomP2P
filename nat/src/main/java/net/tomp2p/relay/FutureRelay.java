package net.tomp2p.relay;

import java.util.Collection;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.BaseFutureImpl;
import net.tomp2p.peers.PeerAddress;

public class FutureRelay extends BaseFutureImpl<FutureRelay> {

	/**
	 * Defines how many relays have to be set up to consider it a success
	 */
	private final int nrRelays;

	private Collection<PeerConnection> relays;
	private DistributedRelay distributedRelay;
	private boolean isRelayNotRequired;

	public FutureRelay() {
		self(this);
		this.nrRelays = PeerAddress.MAX_RELAYS;
	}

	public FutureRelay(int nrRelays) {
		self(this);
		this.nrRelays = nrRelays;
	}

	public int nrRelays() {
		return nrRelays;
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

	public FutureRelay setDone(Collection<PeerConnection> relays) {
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

	public FutureRelay distributedRelay(DistributedRelay distributedRelay) {
		synchronized (lock) {
			this.distributedRelay = distributedRelay;
		}
		return this;
	}

	public DistributedRelay distributedRelay() {
		synchronized (lock) {
			return distributedRelay;
		}

	}
}
