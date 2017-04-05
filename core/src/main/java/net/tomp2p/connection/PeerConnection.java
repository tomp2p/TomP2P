package net.tomp2p.connection;

import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import lombok.Getter;
import lombok.experimental.Accessors;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.peers.PeerAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Thomas Bocek
 */
@Accessors(chain = true, fluent = true)
public class PeerConnection {

    final private static Logger LOG = LoggerFactory.getLogger(PeerConnection.class);
    final public static int HEART_BEAT_MILLIS = 2000;

    @Getter final private PeerAddress remotePeer;
    @Getter final private boolean initiator;

    @Getter final private FutureDone<Void> closeFuture = new FutureDone<Void>();
    @Getter final private int idleMillis;
    //-1 no heartbeat, 0 heartbeat but not done by use, > 0 heartbeat done by us
    @Getter final private int heartBeatMillis;
    @Getter final private ConnectionBean.Protocol protocol;

    @Getter final private ChannelCreator channelCreator;
    
    @Getter private ChannelFuture channelFuture = null;
    @Getter private ChannelCreator.ChannelCloseListener closeListener = null;
    private boolean closeRequested = false;

    public PeerConnection() {
        this.remotePeer = null;
        this.initiator = false;
        this.idleMillis = -1;
        this.heartBeatMillis = -1;
        this.protocol = null;
        this.channelCreator = null;
    }

    private PeerConnection(final PeerAddress remotePeer, final int idleMillis,
            final ConnectionBean.Protocol protocol, final int heartBeatMillis,
            final ChannelFuture channelFuture, ChannelCreator.ChannelCloseListener closeListener, 
            final boolean initiator) {
        this.remotePeer = remotePeer;
        this.initiator = initiator;
        this.idleMillis = idleMillis;
        this.heartBeatMillis = heartBeatMillis;
        this.protocol = protocol;
        channelFuture(channelFuture, closeListener);
        this.channelCreator = null;
    }

    private PeerConnection(final PeerAddress remotePeer, final int idleMillis,
            final ConnectionBean.Protocol protocol, final int heartBeatMillis,
            final ChannelCreator channelCreator, final boolean initiator) {
        this.remotePeer = remotePeer;
        this.initiator = initiator;
        this.idleMillis = idleMillis;
        this.heartBeatMillis = heartBeatMillis;
        this.protocol = protocol;
        this.channelCreator = channelCreator;
    }

    public static PeerConnection newPeerConnectionTCP(final ChannelCreator channelCreator,
            final PeerAddress remotePeer, final int idleMillis) {
        return new PeerConnection(remotePeer, idleMillis, ConnectionBean.Protocol.TCP, -1, channelCreator,
                true);
    }

    public static PeerConnection existingPeerConnectionTCP(final PeerAddress remotePeer, final int idleMillis,
            final ChannelFuture channelFuture, ChannelCreator.ChannelCloseListener closeListener) {
        return new PeerConnection(remotePeer, idleMillis, ConnectionBean.Protocol.TCP, -1, channelFuture, closeListener,
                false);
    }

    public static PeerConnection newPermanentPeerConnectionTCP(final ChannelCreator channelCreator,
            final PeerAddress remotePeer, final int idleMillis, final int heartBeatMillis) {
        return new PeerConnection(remotePeer, idleMillis, ConnectionBean.Protocol.TCP,
                heartBeatMillis, channelCreator, true);
    }

    public static PeerConnection existingPermanentPeerConnectionTCP(final PeerAddress remotePeer,
            final int idleMillis, final ChannelFuture channelFuture, ChannelCreator.ChannelCloseListener closeListener) {
        return new PeerConnection(remotePeer, idleMillis, ConnectionBean.Protocol.TCP, 0, channelFuture, closeListener, false);
    }

    public static PeerConnection newPeerConnectionUDT(final ChannelCreator channelCreator,
            final PeerAddress remotePeer, final int idleMillis) {
        return new PeerConnection(remotePeer, idleMillis, ConnectionBean.Protocol.UDT, -1, channelCreator,
                true);
    }

    public static PeerConnection existingPeerConnectionUDT(final PeerAddress remotePeer, final int idleMillis,
            final ChannelFuture channelFuture, ChannelCreator.ChannelCloseListener closeListener) {
        return new PeerConnection(remotePeer, idleMillis, ConnectionBean.Protocol.UDT, -1, channelFuture, closeListener,
                false);
    }

    public static PeerConnection newPermanentPeerConnectionUDT(final ChannelCreator channelCreator,
            final PeerAddress remotePeer, final int idleMillis, final int heartBeatMillis) {
        return new PeerConnection(remotePeer, idleMillis, ConnectionBean.Protocol.UDT, heartBeatMillis,
                channelCreator,
                true);
    }

    public static PeerConnection existingPermanentPeerConnectionUDT(final PeerAddress remotePeer,
            final int idleMillis, final ChannelFuture channelFuture, ChannelCreator.ChannelCloseListener closeListener) {
        return new PeerConnection(remotePeer, idleMillis, ConnectionBean.Protocol.UDT, 0, channelFuture, closeListener, false);
    }

    public static PeerConnection newPeerConnectionUDP(final ChannelCreator channelCreator,
            final PeerAddress remotePeer, final int idleMillis) {
        return new PeerConnection(remotePeer, idleMillis, ConnectionBean.Protocol.UDP, -1, channelCreator,
                true);
    }

    public static PeerConnection existingPeerConnectionUDP(final PeerAddress remotePeer, final int idleMillis,
            final ChannelFuture channelFuture, ChannelCreator.ChannelCloseListener closeListener) {
        return new PeerConnection(remotePeer, idleMillis, ConnectionBean.Protocol.UDP, -1, channelFuture, closeListener,
                false);
    }

    public PeerConnection channelFuture(final ChannelFuture channelFuture, 
            ChannelCreator.ChannelCloseListener closeListener) {
        synchronized (this) {
            if (this.channelFuture != null && isOpen()) {
                throw new IllegalArgumentException("cannot set twice the channel future");
            }
            if (this.closeListener != null && isOpen()) {
                throw new IllegalArgumentException("cannot set twice the close listener");
            }
            this.closeListener = closeListener;
            this.channelFuture = channelFuture;
            if (closeRequested) {
                channelFuture.channel().close();
            }
        }
        
        closeListener.doneAfterSemaphoreRelease(closeFuture);
        return this;
    }

    public boolean isOpen() {
        synchronized (this) {
            if (channelFuture == null) {
                return false;
            }
            return channelFuture.channel().isOpen();
        }
    }

    public boolean isKeepAlive() {
        return heartBeatMillis >= 0;
    }
    
    public boolean isExisting() {
        return channelCreator == null;
    }
    
    public FutureDone shutdown() {
        FutureDone<Void> done1 = new FutureDone<>();
        if(channelFuture != null) {
            //TODO: this runs in the background
            channelFuture.channel().close().addListener(new GenericFutureListener<Future<? super Void>>() {
                @Override
                public void operationComplete(Future<? super Void> future) throws Exception {
                    done1.done();
                }
            });
        } else {
            done1.done();
        }
        
        FutureDone<Void> done2;
        if(channelCreator != null) {
            done2 = channelCreator.shutdown();
        } else {
            done2 = FutureDone.SUCCESS;
        }
        
        return Futures.whenAll(done1, done2);
    }
    
    public FutureDone<Void> close() {
        synchronized (this) {
            if (channelFuture == null) {
                closeRequested = true;
            } else {
                LOG.debug("Close connection {}. Initiator {}.", initiator, channelFuture);
                channelFuture.channel().close();
            }
        }
        return closeFuture;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("pconn: ");
        sb.append(remotePeer);
        return sb.toString();
    }
}
