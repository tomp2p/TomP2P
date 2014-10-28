package net.tomp2p.nat;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureImpl;
import net.tomp2p.p2p.Shutdown;
import net.tomp2p.relay.FutureRelay;
import net.tomp2p.relay.RelayType;
import net.tomp2p.relay.android.GCMMessageHandler;

public class FutureRelayNAT extends BaseFutureImpl<FutureRelayNAT> {

	private Shutdown shutdown;
	private FutureRelay futureRelay;
	private GCMMessageHandler messageHandler;

	public FutureRelayNAT() {
		self(this);
	}

	public void done(final Shutdown shutdown) {
		synchronized (lock) {
			if (!completedAndNotify()) {
				return;
			}
			this.type = FutureType.OK;
			this.shutdown = shutdown;
		}
		notifyListeners();
	}

	public void done() {
		synchronized (lock) {
			if (!completedAndNotify()) {
				return;
			}
			this.type = FutureType.OK;
			this.shutdown = new Shutdown() {
				@Override
				public BaseFuture shutdown() {
					return FutureRelayNAT.this;
				}
			};
		}
		notifyListeners();

	}

	public Shutdown shutdown() {
		synchronized (lock) {
			return shutdown;
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
	
	public FutureRelayNAT gcmMessageHandler(GCMMessageHandler messageHandler) {
		this.messageHandler = messageHandler;
		return this;
	}
	
	/**
	 * Handles GCM messages when they arrive. Is null except for the case {@link RelayType#ANDROID}
	 */
	public GCMMessageHandler gcmMessageHandler() {
		return messageHandler;
	}
}
