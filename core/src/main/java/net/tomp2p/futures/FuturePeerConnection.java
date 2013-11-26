package net.tomp2p.futures;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.peers.PeerAddress;

public class FuturePeerConnection extends FutureDone<PeerConnection> {
    private final PeerAddress remotePeer;

    public FuturePeerConnection(PeerAddress remotePeer) {
        this.remotePeer = remotePeer;
    }

    public PeerAddress remotePeer() {
        return remotePeer;
    }
    
    public PeerConnection peerConnection() {
        return getObject();
    }

    /**
     * @return The future when this close is complete.
     */
    public FutureDone<Void> close() {
        final FutureDone<Void> shutdown = new FutureDone<Void>();
        addListener(new BaseFutureAdapter<FutureDone<PeerConnection>>() {
            @Override
            public void operationComplete(final FutureDone<PeerConnection> future) throws Exception {
                if (future.isSuccess()) {
                    future.getObject().close().addListener(new BaseFutureAdapter<FutureDone<Void>>() {
                        @Override
                        public void operationComplete(final FutureDone<Void> future) throws Exception {
                            if (future.isSuccess()) {
                                shutdown.setDone();
                            } else {
                                shutdown.setFailed("could not close (1)", future);
                            }
                        }
                    });
                } else {
                    shutdown.setFailed("could not close (2)", future);
                }
            }
        });
        return shutdown;
    }
}
