package net.tomp2p.nat;

import net.tomp2p.futures.BaseFutureImpl;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.p2p.Shutdown;
import net.tomp2p.relay.FutureRelay;

public class FutureRelayNAT extends BaseFutureImpl<FutureRelayNAT> {

	private Shutdown shutdown;
	private FutureBootstrap futureBootstrap0;
	private FutureBootstrap futureBootstrap1;
	private FutureRelay futureRelay;

	public FutureRelayNAT() {
		self(this);
	}

	public void done(final Shutdown shutdown) {
		synchronized (lock) {
			if (!setCompletedAndNotify()) {
				return;
			}
			this.type = FutureType.OK;
			this.shutdown = shutdown;
		}
		notifyListeners();
	}

	public Shutdown shutdown() {
		synchronized (lock) {
			return shutdown;
		}
	}

	public FutureRelayNAT futureBootstrap1(FutureBootstrap futureBootstrap1) {
		synchronized (lock) {
			this.futureBootstrap1 = futureBootstrap1;
		}
		return this;
	}

	public FutureBootstrap futureBootstrap1() {
		synchronized (lock) {
			return futureBootstrap1;
		}
	}

	public FutureRelayNAT futureRelay(FutureRelay futureRelay) {
		synchronized (lock) {
			this.futureRelay = futureRelay;
		}
		return this;
	}

	public FutureRelay futureRelay() {
		synchronized (lock) {
			return futureRelay;
		}
	}

	public FutureRelayNAT futureBootstrap0(FutureBootstrap futureBootstrap0) {
		synchronized (lock) {
			this.futureBootstrap0 = futureBootstrap0;
		}
		return this;
	}

	public FutureBootstrap futureBootstrap0() {
		synchronized (lock) {
			return futureBootstrap0;
		}
	}

}
