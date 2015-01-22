package net.tomp2p.nat;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureImpl;
import net.tomp2p.p2p.Shutdown;
import net.tomp2p.relay.FutureRelay;
import net.tomp2p.relay.buffer.BufferRequestListener;

public class FutureRelayNAT extends BaseFutureImpl<FutureRelayNAT> {

	private Shutdown shutdown;
	private FutureRelay futureRelay;
	private BufferRequestListener bufferRequestListener;

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
	
	public FutureRelayNAT gcmMessageHandler(BufferRequestListener bufferRequestListener) {
		this.bufferRequestListener = bufferRequestListener;
		return this;
	}
	
	public BufferRequestListener gcmMessageHandler() {
		return bufferRequestListener;
	}
}
