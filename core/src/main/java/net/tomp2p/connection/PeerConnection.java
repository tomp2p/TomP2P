package net.tomp2p.connection;

import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.peers.PeerAddress;

public class PeerConnection {

    final private Semaphore oneConnection = new Semaphore(1);

    final private PeerAddress remotePeer;
    final private ChannelCreator cc;
    
    final private Map<FutureChannelCreator, FutureResponse> map = new LinkedHashMap<FutureChannelCreator, FutureResponse>();

    // these may be called from different threads, but they will never be called concurrently within this library
    private volatile FutureResponse futureResponse;
    private volatile ChannelFuture channelFuture;

    public PeerConnection(PeerAddress remotePeer, ChannelCreator cc) {
        this.remotePeer = remotePeer;
        this.cc = cc;
    }

    public PeerConnection(PeerAddress remotePeer, ChannelFuture channelFuture) {
        this.remotePeer = remotePeer;
        this.channelFuture = channelFuture;
        this.cc = null;

    }

    public FutureDone<Void> close() {
        // cc is not null if we opened the connection
        if (cc != null) {
            return cc.shutdown();
        } else {
            // cc is null if its an incoming connection. We can close it here, or it will be closed when the dispatcher
            // is shutdown
            final FutureDone<Void> futureDone = new FutureDone<Void>();
            channelFuture.channel().close().addListener(new GenericFutureListener<Future<? super Void>>() {
                @Override
                public void operationComplete(Future<? super Void> future) throws Exception {
                    futureDone.setDone();
                }
            });
            return futureDone;
        }
    }
    
    public FutureChannelCreator acquire(final FutureResponse futureResponse) {
        FutureChannelCreator futureChannelCreator = new FutureChannelCreator();
        return acquire(futureChannelCreator, futureResponse);
    }
    
    private FutureChannelCreator acquire(final FutureChannelCreator futureChannelCreator, final FutureResponse futureResponse) {
        if (oneConnection.tryAcquire()) {
            futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
                @Override
                public void operationComplete(FutureResponse future) throws Exception {
                    oneConnection.release();
                    synchronized (map) {
                        Iterator<Map.Entry<FutureChannelCreator, FutureResponse>> iterator = map.entrySet().iterator();
                        if(iterator.hasNext()) {
                            Map.Entry<FutureChannelCreator, FutureResponse> entry = iterator.next();
                            iterator.remove();
                            acquire(entry.getKey(), entry.getValue());
                        }
                    }
                }
            });
            this.futureResponse = futureResponse;
            futureChannelCreator.reserved(cc);
            return futureChannelCreator;
        } else {
            synchronized (map) {
                map.put(futureChannelCreator, futureResponse);
            }
        }
        return futureChannelCreator;
    }
    
    public ChannelCreator channelCreator() {
        return cc;
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
