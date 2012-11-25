package net.tomp2p.rpc;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.storage.Data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SenderCacheStrategy
{
    final private static Logger LOGGER = LoggerFactory.getLogger( SenderCacheStrategy.class );

    final private int delay;

    final private int size;

    final private ScheduledExecutorService executor = Executors.newScheduledThreadPool( 1 );

    final private Map<CacheKey, RequestWrapper> cache = new HashMap<CacheKey, RequestWrapper>();

    public SenderCacheStrategy( int delay, int size )
    {
        this.delay = delay;
        this.size = size;
    }

    public FutureResponse putIfAbsent( CacheKey cacheKey, RequestHandlerTCP<FutureResponse> request,
                                       ChannelCreator channelCreator )
    {
        synchronized ( cache )
        {
            if ( cache.containsKey( cacheKey ) )
            {
                RequestWrapper requestWrapper = cache.get( cacheKey );
                RequestHandlerTCP<FutureResponse> requestHandlerTCP = requestWrapper.getRequest();
                requestHandlerTCP.getFutureResponse().share();
                int newSize =
                    merge( request.getFutureResponse().getRequest(), requestHandlerTCP.getFutureResponse().getRequest() );
                if ( newSize > size )
                {
                    fireMessage( cacheKey, requestWrapper );
                    if ( LOGGER.isDebugEnabled() )
                    {
                        LOGGER.debug( "fire request, size reached" );
                    }
                }
                else
                {
                    SendTimer sendTimer = requestWrapper.getSendTimer();
                    sendTimer.activity();
                    request.getFutureResponse().setResponse();
                    if ( LOGGER.isDebugEnabled() )
                    {
                        LOGGER.debug( "caching request" );
                    }
                }

                return requestHandlerTCP.getFutureResponse();
            }
            else
            {
                RequestWrapper requestWrapper = new RequestWrapper();
                requestWrapper.setRequest( request );
                requestWrapper.setChannelCreator( channelCreator );
                SendTimer sendTimer = new SendTimer( delay, cacheKey, requestWrapper );
                requestWrapper.setSendTimer( sendTimer );
                ScheduledFuture<?> future = executor.schedule( sendTimer, delay, TimeUnit.MILLISECONDS );
                requestWrapper.setFuture( future );
                cache.put( cacheKey, requestWrapper );
                if ( LOGGER.isDebugEnabled() )
                {
                    LOGGER.debug( "new request to " + cacheKey.getRemotePeer() );
                }
            }
        }
        return request.getFutureResponse();
    }

    private void fireMessage( CacheKey cacheKey, RequestWrapper requestWrapper )
    {
        cache.remove( cacheKey );
        ScheduledFuture<?> future = requestWrapper.getFuture();
        future.cancel( false );
        requestWrapper.getRequest().sendTCP( requestWrapper.getChannelCreator() );
        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "fire message" );
        }
    }

    private int merge( Message requestSource, Message requestTarget )
    {
        Map<Number480, Data> dataMap480;
        if ( requestTarget.getDataMap480() == null )
        {
            dataMap480 = new HashMap<Number480, Data>();
            convertDataMap( requestTarget, dataMap480 );
        }
        else
        {
            dataMap480 = requestTarget.getDataMap480();
        }
        if ( requestSource.getDataMap() != null )
        {
            convertDataMap( requestSource, dataMap480 );
        }
        if ( requestSource.getDataMap480() != null )
        {
            dataMap480.putAll( requestSource.getDataMap480() );
        }
        return dataMap480.size();
    }

    private void convertDataMap( Message requestTarget, Map<Number480, Data> dataMap )
    {
        Number160 locationKey = requestTarget.getKeyKey1();
        Number160 domainKey = requestTarget.getKeyKey2();

        for ( Map.Entry<Number160, Data> entry : requestTarget.getDataMap().entrySet() )
        {
            dataMap.put( new Number480( locationKey, domainKey, entry.getKey() ), entry.getValue() );
        }
        requestTarget.replaceDataMap( dataMap );
    }

    private class SendTimer
        implements Runnable
    {
        private long lastActivity = System.currentTimeMillis();

        final private int delay;

        final private RequestWrapper requestWrapper;

        final private CacheKey cacheKey;

        public SendTimer( int delay, CacheKey cacheKey, RequestWrapper requestWrapper )
        {
            this.delay = delay;
            this.cacheKey = cacheKey;
            this.requestWrapper = requestWrapper;
        }

        public void activity()
        {
            lastActivity = System.currentTimeMillis();
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "last activity: " + lastActivity );
            }
        }

        @Override
        public void run()
        {
            final long now = System.currentTimeMillis();
            synchronized ( cache )
            {

                if ( now - lastActivity > delay && cache.containsKey( cacheKey ) )
                {
                    LOGGER.debug( "fire message, as timeout occured, delay: " + ( now - lastActivity ) );
                    fireMessage( cacheKey, requestWrapper );

                }
                else
                {
                    ScheduledFuture<?> future =
                        executor.schedule( this, delay - ( now - lastActivity ), TimeUnit.MILLISECONDS );
                    requestWrapper.setFuture( future );
                }
            }
        }
    }

    private static class RequestWrapper
    {
        private RequestHandlerTCP<FutureResponse> request;

        private SendTimer sendTimer;

        private ScheduledFuture<?> future;

        private ChannelCreator channelCreator;

        public RequestHandlerTCP<FutureResponse> getRequest()
        {
            return request;
        }

        public void setRequest( RequestHandlerTCP<FutureResponse> request )
        {
            this.request = request;
        }

        public SendTimer getSendTimer()
        {
            return sendTimer;
        }

        public void setSendTimer( SendTimer sendTimer )
        {
            this.sendTimer = sendTimer;
        }

        public ScheduledFuture<?> getFuture()
        {
            return future;
        }

        public void setFuture( ScheduledFuture<?> future )
        {
            this.future = future;
        }

        public ChannelCreator getChannelCreator()
        {
            return channelCreator;
        }

        public void setChannelCreator( ChannelCreator channelCreator )
        {
            this.channelCreator = channelCreator;
        }
    }
}
