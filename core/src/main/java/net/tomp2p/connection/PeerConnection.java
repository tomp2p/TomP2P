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
	final public static int HEART_BEAT_MILLIS = 2000;
    final private Semaphore oneConnection = new Semaphore(1);

    final private PeerAddress remotePeer;
    final private ChannelCreator cc;

    final private Map<FutureChannelCreator, FutureResponse> map = new LinkedHashMap<FutureChannelCreator, FutureResponse>();
    final private FutureDone<Void> closeFuture = new FutureDone<Void>();
    private final int heartBeatMillis;

    // these may be called from different threads, but they will never be called concurrently within this library
    private volatile ChannelFuture channelFuture;
    

    /**
     * If we don't have an open TCP connection, we first need a channel creator to open a channel.
     * 
     * @param remotePeer
     *            The remote peer to connect to
     * @param cc
     *            The channel creator where we can open a TCP connection
     */
    public PeerConnection(PeerAddress remotePeer, ChannelCreator cc, int heartBeatMillis) {
        this.remotePeer = remotePeer;
        this.cc = cc;
        this.heartBeatMillis = heartBeatMillis;
    }

    /**
     * If we already have an open TCP connection, we don't need a channel creator
     * 
     * @param remotePeer
     *            The remote peer to connect to
     * @param channelFuture
     *            The channel future of an already open TCP connection
     */
    public PeerConnection(PeerAddress remotePeer, ChannelFuture channelFuture, int heartBeatMillis) {
        this.remotePeer = remotePeer;
        this.channelFuture = channelFuture;
        addCloseListener(channelFuture);
        this.cc = null;
        this.heartBeatMillis = heartBeatMillis;
    }

    public PeerConnection channelFuture(ChannelFuture channelFuture) {
        this.channelFuture = channelFuture;
        addCloseListener(channelFuture);
        return this;
    }
    
    public int heartBeatMillis() {
	    return heartBeatMillis;
    }

    public ChannelFuture channelFuture() {
        return channelFuture;
    }

    public FutureDone<Void> closeFuture() {
        return closeFuture;
    }

    private void addCloseListener(ChannelFuture channelFuture2) {
        channelFuture.channel().closeFuture().addListener(new GenericFutureListener<Future<? super Void>>() {
            @Override
            public void operationComplete(Future<? super Void> arg0) throws Exception {
                closeFuture.setDone();
            }
        });
    }

    public FutureDone<Void> close() {
        // cc is not null if we opened the connection
        if (cc != null) {
            FutureDone<Void> future = cc.shutdown();
            // Maybe done on arrival? Set close future in any case
            future.addListener(new BaseFutureAdapter<FutureDone<Void>>() {
                @Override
                public void operationComplete(FutureDone<Void> future) throws Exception {
                    closeFuture.setDone();
                }
            });        
        } else {
            // cc is null if its an incoming connection. We can close it here, or it will be closed when the dispatcher
            // is shutdown
            channelFuture.channel().close();
        }
        return closeFuture;
    }

    public FutureChannelCreator acquire(final FutureResponse futureResponse) {
        FutureChannelCreator futureChannelCreator = new FutureChannelCreator();
        return acquire(futureChannelCreator, futureResponse);
    }

    private FutureChannelCreator acquire(final FutureChannelCreator futureChannelCreator,
            final FutureResponse futureResponse) {
        if (oneConnection.tryAcquire()) {
            futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
                @Override
                public void operationComplete(FutureResponse future) throws Exception {
                    oneConnection.release();
                    synchronized (map) {
                        Iterator<Map.Entry<FutureChannelCreator, FutureResponse>> iterator = map.entrySet()
                                .iterator();
                        if (iterator.hasNext()) {
                            Map.Entry<FutureChannelCreator, FutureResponse> entry = iterator.next();
                            iterator.remove();
                            acquire(entry.getKey(), entry.getValue());
                        }
                    }
                }
            });
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
}
