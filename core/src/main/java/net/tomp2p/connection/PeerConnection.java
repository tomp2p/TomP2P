package net.tomp2p.connection;

import io.netty.channel.ChannelFuture;

import java.util.concurrent.Semaphore;

import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.peers.PeerAddress;

public class PeerConnection {

    final private Semaphore oneConnection = new Semaphore(1);

    final private PeerAddress remotePeer;
    final private ChannelCreator cc;

    //these may be called from different threads, but they will never be called concurrently within this library
    private volatile FutureResponse futureResponse;
    private volatile ChannelFuture channelFuture;

    public PeerConnection(PeerAddress remotePeer, ChannelCreator cc) {
        this.remotePeer = remotePeer;
        this.cc = cc;
    }

    public FutureDone<Void> close() {
        return cc.shutdown();
    }

    public ChannelCreator acquire(final FutureResponse futureResponse) {
        if (oneConnection.tryAcquire()) {
            this.futureResponse = futureResponse;
            return cc;
        } else {
            return null;
        }
    }

    public PeerConnection release() {
        oneConnection.release();
        return this;
    }

    public PeerAddress remotePeer() {
        return remotePeer;
    }

    public ChannelFuture channelFuture() {
        return channelFuture;
    }

    public PeerConnection channelFuture(ChannelFuture channelFuture) {
        this.channelFuture = channelFuture;
        return this;
    }

    /**
     * @return The current future that is being used. If you try to call {@link #acquire(FutureResponse)} and the other
     *         future has not finished yet, the new future will be ignored. This method may return null if no future has
     *         been set. Once this connection is used, there will always be a future response.
     */
    public FutureResponse currentFutureResponse() {
        return futureResponse;
    }
}
