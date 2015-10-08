package net.tomp2p.connection;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Stripped-down version of the IdleStateHandler.
 */
public class IdleStateHandlerTomP2P extends ChannelDuplexHandler {

    private final long allIdleTimeMillis;

    private volatile long lastReadTime;

    private volatile long lastWriteTime;

    private volatile ScheduledFuture<?> allIdleTimeout;

    private volatile int state; // 0 - none, 1 - initialized, 2 - destroyed

    /**
     * Creates a new instance firing {@link IdleStateEvent}s.
     * 
     * @param allIdleTimeSeconds
     *            an {@link IdleStateEvent} whose state is {@link IdleState#ALL_IDLE} will be triggered when neither
     *            read nor write was performed for the specified period of time. Specify {@code 0} to disable.
     */
    public IdleStateHandlerTomP2P(int allIdleTimeMillis) {

        this(allIdleTimeMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Creates a new instance firing {@link IdleStateEvent}s.
     * 
     * @param allIdleTime
     *            an {@link IdleStateEvent} whose state is {@link IdleState#ALL_IDLE} will be triggered when neither
     *            read nor write was performed for the specified period of time. Specify {@code 0} to disable.
     * @param unit
     *            the {@link TimeUnit} of {@code readerIdleTime}, {@code writeIdleTime}, and {@code allIdleTime}
     */
    public IdleStateHandlerTomP2P(long allIdleTime, TimeUnit unit) {
        if (unit == null) {
            throw new NullPointerException("unit");
        }
        if (allIdleTime <= 0) {
            allIdleTimeMillis = 0;
        } else {
            allIdleTimeMillis = Math.max(unit.toMillis(allIdleTime), 1);
        }
    }

    /**
     * Return the allIdleTime that was given when instance this class in milliseconds.
     * 
     */
    public long getAllIdleTimeInMillis() {
        return allIdleTimeMillis;
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
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        destroy();
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
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        destroy();
        super.channelInactive(ctx);
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

    private void initialize(ChannelHandlerContext ctx) {
        // Avoid the case where destroy() is called before scheduling timeouts.
        // See: https://github.com/netty/netty/issues/143
        switch (state) {
        case 1:
        case 2:
            return;
        }

        state = 1;

        lastReadTime = lastWriteTime = System.currentTimeMillis();

        if (allIdleTimeMillis > 0) {
            allIdleTimeout = ctx.executor().schedule(new AllIdleTimeoutTask(ctx), allIdleTimeMillis,
                    TimeUnit.MILLISECONDS);
        }
    }

    private void destroy() {
        state = 2;

        if (allIdleTimeout != null) {
            allIdleTimeout.cancel(false);
            allIdleTimeout = null;
        }
    }

    private final class AllIdleTimeoutTask implements Runnable {

        private final ChannelHandlerContext ctx;

        AllIdleTimeoutTask(ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void run() {
            if (!ctx.channel().isOpen()) {
                return;
            }

            long currentTime = System.currentTimeMillis();
            long lastIoTime = Math.max(lastReadTime, lastWriteTime);
            long nextDelay = allIdleTimeMillis - (currentTime - lastIoTime);
            if (nextDelay <= 0) {
                // Both reader and writer are idle - set a new timeout and
                // notify the callback.
                allIdleTimeout = ctx.executor().schedule(this, allIdleTimeMillis, TimeUnit.MILLISECONDS);
                try {
                    channelIdle(ctx);
                } catch (Throwable t) {
                    ctx.fireExceptionCaught(t);
                }
            } else {
                // Either read or write occurred before the timeout - set a new
                // timeout with shorter delay.
                allIdleTimeout = ctx.executor().schedule(this, nextDelay, TimeUnit.MILLISECONDS);
            }
        }
    }

    private void channelIdle(ChannelHandlerContext ctx) throws Exception {
        ctx.fireUserEventTriggered(this);
    }
}
