package relay;

import net.tomp2p.futures.BaseFutureImpl;
import net.tomp2p.futures.FuturePeerConnection;

public class RelayConnectionFuture extends BaseFutureImpl<RelayConnectionFuture> {
	
	private FuturePeerConnection futurePeerConnection;
	
	
	public FuturePeerConnection futurePeerConnection() {
		return futurePeerConnection;
	}
	
	public void setPeerConnection(FuturePeerConnection fpc) {
		futurePeerConnection = fpc;
	}
	
	public void done() {
		synchronized (lock) {
			setCompletedAndNotify();
		}
		notifyListeners();
	}

}
