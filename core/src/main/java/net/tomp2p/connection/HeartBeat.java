package net.tomp2p.connection;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.EventExecutor;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.p2p.builder.PingBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stripped-down version of the IdleStateHandler.
 */
public class HeartBeat extends ChannelDuplexHandler {
	
	private static int MIN_TIME_TO_HEARTBEAT_MILLIS = 500;
    
    private static final Logger LOG = LoggerFactory.getLogger(HeartBeat.class);

    private final long timeToHeartBeatMillis;
    
    private volatile long lastReadTime;

    private volatile long lastWriteTime;
    
    private volatile ScheduledFuture<?> heartBeatFuture;
    
    private volatile int state; // 0 - none, 1 - initialized, 2 - destroyed
    
    private final PingBuilderFactory pingBuilderFactory;
    //may be set from other threads
    private volatile PeerConnection peerConnection;

    /**
     * Creates a new instance firing {@link IdleStateEvent}s.
     * 
     * @param allIdleTime
     *            an {@link IdleStateEvent} whose state is {@link IdleState#ALL_IDLE} will be triggered when neither
     *            read nor write was performed for the specified period of time. Specify {@code 0} to disable.
     * @param unit
     *            the {@link TimeUnit} of {@code readerIdleTime}, {@code writeIdleTime}, and {@code allIdleTime}
     * @param pingBuilderFactory
     *            the pingBuilderFactory
     */
    public HeartBeat(long allIdleTime, TimeUnit unit, PingBuilderFactory pingBuilderFactory) {
        if (unit == null) {
            throw new NullPointerException("unit");
        }
        if (allIdleTime <= 0) {
            timeToHeartBeatMillis = 0;
        } else {
            timeToHeartBeatMillis = Math.max(unit.toMillis(allIdleTime), MIN_TIME_TO_HEARTBEAT_MILLIS);
        }
        this.pingBuilderFactory = pingBuilderFactory;
    }
    
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        lastReadTime = System.currentTimeMillis();
        ctx.fireChannelRead(msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        promise.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                lastWriteTime = System.currentTimeMillis();
            }
        });
        ctx.write(msg, promise);
    }

    /**
     * Return the allIdleTime that was given when instance this class in milliseconds.
     * 
     */
    public long getAllIdleTimeInMillis() {
        return timeToHeartBeatMillis;
    }
    
    public PeerConnection peerConnection() {
        return peerConnection;
    }
    
    public HeartBeat peerConnection(PeerConnection peerConnection) {
        this.peerConnection = peerConnection;
        return this;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        if (ctx.channel().isActive() && ctx.channel().isRegistered()) {
            // channelActive() event has been fired already, which means this.channelActive() will
            // not be invoked. We have to initialize here instead.
            initialize(ctx);
        } else {
            // channelActive() event has not been fired yet. this.channelActive() will be invoked
            // and initialization will occur there.
        }
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        // Initialize early if channel is active already.
        if (ctx.channel().isActive()) {
            initialize(ctx);
        }
        super.channelRegistered(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // This method will be invoked only if this handler was added
        // before channelActive() event is fired. If a user adds this handler
        // after the channelActive() event, initialize() will be called by beforeAdd().
        initialize(ctx);
        super.channelActive(ctx);
    }
    
    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        destroy();
    }
    
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        destroy();
        super.channelInactive(ctx);
    }

    private void initialize(ChannelHandlerContext ctx) {
        
        // Avoid the case where destroy() is called before scheduling timeouts.
        // See: https://github.com/netty/netty/issues/143
        switch (state) {
        case 1:
        case 2:
            return;
        }

        state = 1;

        EventExecutor loop = ctx.executor();

        lastReadTime = lastWriteTime = System.currentTimeMillis();
        
        heartBeatFuture = loop.scheduleAtFixedRate(new HeartBeating(ctx), timeToHeartBeatMillis, timeToHeartBeatMillis, TimeUnit.MILLISECONDS);
    }
    
    private void destroy() {
        state = 2;

        if (heartBeatFuture != null) {
            heartBeatFuture.cancel(false);
            heartBeatFuture = null;
        }
    }

    private final class HeartBeating implements Runnable {

        private final ChannelHandlerContext ctx;

        HeartBeating(ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void run() {
            if (!ctx.channel().isOpen()) {
                return;
            }
            
            long currentTime = System.currentTimeMillis();
            long lastIoTime = Math.max(lastReadTime, lastWriteTime);
            long nextDelay = timeToHeartBeatMillis - (currentTime - lastIoTime);
            
            if(peerConnection!=null && nextDelay <= 0) {
                LOG.debug("Sending heart beat to {}. Channel: {}.", peerConnection.remotePeer(),
                		(peerConnection.channelFuture() != null ? peerConnection.channelFuture().channel() : null));
                PingBuilder builder = pingBuilderFactory.create();
                BaseFuture baseFuture = builder.peerConnection(peerConnection).start();
                builder.notifyAutomaticFutures(baseFuture);
            } else {
            	LOG.debug("Not sending heart beat to {}. Channel: {}", peerConnection.remotePeer(),
                		(peerConnection.channelFuture() != null ? peerConnection.channelFuture().channel() : null));
            }
        }
    }
}
